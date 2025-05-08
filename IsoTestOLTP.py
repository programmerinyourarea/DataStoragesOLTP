# Google Colab setup: install dependencies
from datetime import datetime, timedelta
import threading, time

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey, Numeric,
    MetaData, text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import insert as pg_insert

# --- Configuration ---
DB_URL = (
    connect db here
)
SCHEMA = 'HashGuess'

engine = create_engine(DB_URL, echo=False)
metadata = MetaData(schema=SCHEMA)
Base = declarative_base(metadata=metadata)
Session = sessionmaker(bind=engine)

# --- 1) Schema & Table Setup ---
def reset_schema():
    """Drop and recreate our isolated schema."""
    with engine.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {SCHEMA}"))
        conn.commit()

class Player(Base):
    __tablename__ = 'player'
    player_id = Column(Integer, primary_key=True)
    username  = Column(String, unique=True, nullable=False)
    email     = Column(String, unique=True, nullable=False)

class Subscription(Base):
    __tablename__ = 'subscription'
    subscription_id = Column(Integer, primary_key=True)
    player_id       = Column(Integer, ForeignKey(f"{SCHEMA}.player.player_id"), nullable=False)
    start_date      = Column(DateTime, nullable=False)
    end_date        = Column(DateTime, nullable=False)
    status          = Column(String, nullable=False)

class Payment(Base):
    __tablename__ = 'payment'
    payment_id = Column(Integer, primary_key=True)
    player_id  = Column(Integer, ForeignKey(f"{SCHEMA}.player.player_id"), nullable=False)
    amount     = Column(Numeric, nullable=False)
    timestamp  = Column(DateTime, default=datetime.utcnow)

class BlockNumber(Base):
    __tablename__ = 'blocknumber'
    block_number_id = Column(Integer, primary_key=True)
    block_hash      = Column(String, unique=True, nullable=False)
    actual_value    = Column(String, nullable=False)
    timestamp       = Column(DateTime, default=datetime.utcnow)

class Bet(Base):
    __tablename__ = 'bet'
    bet_id            = Column(Integer, primary_key=True)
    player_id         = Column(Integer, ForeignKey(f"{SCHEMA}.player.player_id"), nullable=False)
    block_number_id   = Column(Integer, ForeignKey(f"{SCHEMA}.blocknumber.block_number_id"), nullable=False)
    prediction        = Column(String, nullable=False)
    timestamp         = Column(DateTime, default=datetime.utcnow)

class Result(Base):
    __tablename__ = 'result'
    result_id = Column(Integer, primary_key=True)
    bet_id     = Column(Integer, ForeignKey(f"{SCHEMA}.bet.bet_id"), nullable=False, unique=True)
    is_win     = Column(Boolean, nullable=False)


def create_tables():
    reset_schema()
    Base.metadata.create_all(engine)

# --- 2) Sample Data Population ---
def populate_sample_data():
    s = Session()
    # Upsert players
    stmt = pg_insert(Player).values([
        {'username':'alice','email':'alice@example.com'},
        {'username':'bob','email':'bob@example.com'}
    ])
    stmt = stmt.on_conflict_do_nothing(index_elements=['username'])
    s.execute(stmt)
    s.commit()

    alice = s.query(Player).filter_by(username='alice').one()
    # Subscription
    if not s.query(Subscription).filter_by(player_id=alice.player_id).first():
        s.add(Subscription(
            player_id=alice.player_id,
            start_date=datetime.utcnow(),
            end_date=datetime.utcnow()+timedelta(days=30),
            status='active'
        ))
    # Payment
    if not s.query(Payment).filter_by(player_id=alice.player_id, amount=10).first():
        s.add(Payment(player_id=alice.player_id, amount=10))

    # Block, Bet, Result
    blk_stmt = pg_insert(BlockNumber).values(
        block_hash='000abc', actual_value='c'
    ).on_conflict_do_nothing(index_elements=['block_hash'])
    s.execute(blk_stmt); s.commit()
    block = s.query(BlockNumber).filter_by(block_hash='000abc').one()

    if not s.query(Bet).filter_by(
        player_id=alice.player_id, block_number_id=block.block_number_id
    ).first():
        bet = Bet(
            player_id=alice.player_id,
            block_number_id=block.block_number_id,
            prediction='c'
        )
        s.add(bet); s.flush()
        s.add(Result(bet_id=bet.bet_id, is_win=True))

    s.commit(); s.close()

# --- 3) Business Operations ---
def op_create_player(username, email):
    """Create or return existing player."""
    s = Session()
    stmt = pg_insert(Player).values(username=username, email=email)
    stmt = stmt.on_conflict_do_nothing(index_elements=['username'])
    s.execute(stmt); s.commit()
    p = s.query(Player).filter_by(username=username).one()
    s.close(); return p


def op_place_bet(player_id, block_hash, prediction):
    """Thread-safe insert of BlockNumber + place a bet and return is_win."""
    s = Session()

    # Ensure BlockNumber exists using upsert
    insert_stmt = pg_insert(BlockNumber).values(
        block_hash=block_hash,
        actual_value=block_hash[-1]
    ).on_conflict_do_nothing(index_elements=['block_hash'])
    s.execute(insert_stmt)
    s.commit()  # ensure visibility

    blk = s.query(BlockNumber).filter_by(block_hash=block_hash).one()
    bet = Bet(player_id=player_id, block_number_id=blk.block_number_id, prediction=prediction)
    s.add(bet)
    s.flush()
    is_win = (prediction == blk.actual_value)
    s.add(Result(bet_id=bet.bet_id, is_win=is_win))
    s.commit()
    s.close()
    return is_win


# --- 4) Performance Tests ---
def perf_test_players(n=100000):
    """Single bulk-insert of players."""
    s = Session()
    stmt = pg_insert(Player).values([
        {'username':f'u{i}', 'email':f'u{i}@ex.com'} for i in range(n)
    ])
    stmt = stmt.on_conflict_do_nothing(index_elements=['username'])
    start = time.time(); s.execute(stmt); s.commit();
    print(f"Inserted {n} in {time.time()-start:.2f}s")
    s.close()


def perf_test_bets(n=100000):
    """Bulk-insert blocks, bets, results with correct foreign keys without dropping schema."""
    s = Session()
    # 0) Clean target tables and reset sequences
    s.execute(text(f"TRUNCATE \"{SCHEMA}\".bet, \"{SCHEMA}\".result, \"{SCHEMA}\".blocknumber RESTART IDENTITY CASCADE"))
    s.commit()

    # 1) Bulk insert blocks
    blocks = [{'block_hash': f'h{i}', 'actual_value': str(i % 16)} for i in range(n)]
    stmt_b = pg_insert(BlockNumber).values(blocks).on_conflict_do_nothing(index_elements=['block_hash'])
    start = time.time()
    s.execute(stmt_b)
    s.commit()

    # 2) Fetch generated block IDs in order
    ids = [bid for (bid,) in s.query(BlockNumber.block_number_id).order_by(BlockNumber.block_number_id).all()]

    # 3) Bulk insert bets
    bets = [
        {'player_id': 1, 'block_number_id': ids[i], 'prediction': str(i % 16)}
        for i in range(len(ids))
    ]
    s.execute(pg_insert(Bet).values(bets))
    s.commit()

    # 4) Bulk insert results
    results = [
        {'bet_id': i+1, 'is_win': (bets[i]['prediction'] == str((i) % 16))}
        for i in range(len(ids))
    ]
    s.execute(pg_insert(Result).values(results))
    s.commit()

    elapsed = time.time() - start
    print(f"Processed {n} bets+results in {elapsed:.2f}s (~{n/elapsed:.0f} ops/s)")
    s.close()


def isolation_test():
    """Run two parallel bets on same block to verify isolation; fetch and print boolean outcomes._"""
    results = []
    lock = threading.Lock()
    def worker(pred):
        # Place bet, which returns a Result instance
        res = op_place_bet(1, 'isolated', pred)
        # After placing, re-query the is_win from a fresh session to avoid detached errors
        session = Session()
        # Find the latest Result for this block and player
        # Assuming bet_id increments, fetch max bet_id for this player and block
        latest = (session.query(Result)
                  .join(Bet, Result.bet_id == Bet.bet_id)
                  .filter(Bet.player_id == 1, Bet.block_number_id == session.query(BlockNumber.block_number_id).filter_by(block_hash='isolated').scalar())
                  .order_by(Result.result_id.desc())
                  .first())
        is_win = latest.is_win
        session.close()
        with lock:
            results.append((pred, is_win))
    threads = [threading.Thread(target=worker, args=(p,)) for p in ('d','b')]
    for t in threads: t.start()
    for t in threads: t.join()
    # Print outcomes
    for pred, win in results:
        print(f"Thread {pred}: win? {win}")


create_tables()
populate_sample_data()    

perf_test_players(50000)

perf_test_bets(50000)

isolation_test()