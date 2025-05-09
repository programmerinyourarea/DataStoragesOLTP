from datetime import datetime
from decimal import Decimal
import uuid, random

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Boolean,
    ForeignKey, Numeric, MetaData, text
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.dialects.postgresql import insert as pg_insert
DB_URL = (
    'postgresql+psycopg2://student:HSBCN323TestDb36111'
    '@database-1.cffitrsftriq.eu-central-1.rds.amazonaws.com:5432/postgres'
)
SCHEMA = 'hashguess'
engine = create_engine(DB_URL, isolation_level="SERIALIZABLE")
metadata = MetaData(schema=SCHEMA)
Base = declarative_base(metadata=metadata)
Session = scoped_session(sessionmaker(bind=engine))

class Player(Base):
    __tablename__ = 'player'
    player_id = Column(Integer, primary_key=True)
    username  = Column(String, unique=True, nullable=False)
    email     = Column(String, unique=True, nullable=False)
    balance   = Column(Numeric(12,2), nullable=False, default=Decimal('0.00'))

class Block(Base):
    __tablename__ = 'block'
    block_id     = Column(Integer, primary_key=True)
    block_hash   = Column(String, unique=True, nullable=True)   # null until generated
    actual_char  = Column(String(1), nullable=True)             # null until generated
    created_at   = Column(DateTime, default=datetime.utcnow, nullable=False)

class Bet(Base):
    __tablename__ = 'bet'
    bet_id       = Column(Integer, primary_key=True)
    player_id    = Column(Integer, ForeignKey(f"{SCHEMA}.player.player_id"), nullable=False)
    block_id     = Column(Integer, ForeignKey(f"{SCHEMA}.block.block_id"), nullable=False)
    prediction   = Column(String(1), nullable=False)
    stake        = Column(Numeric(12,2), nullable=False)
    placed_at    = Column(DateTime, default=datetime.utcnow, nullable=False)
    resolved     = Column(Boolean, default=False, nullable=False)
    is_win       = Column(Boolean, nullable=True)
def reset_schema():
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {SCHEMA}"))
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(text(f"""
        CREATE FUNCTION {SCHEMA}.check_active() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE cnt INT;
        BEGIN
          SELECT COUNT(*) INTO cnt
            FROM {SCHEMA}.bet
           WHERE player_id = NEW.player_id
             AND resolved = FALSE;
          IF cnt > 5 THEN
            RAISE EXCEPTION 'Too many active bets for player %', NEW.player_id;
          END IF;
          RETURN NEW;
        END;
        $$;
        CREATE TRIGGER trg_check_active
          BEFORE INSERT ON {SCHEMA}.bet
          FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.check_active();
        """))
# 1) Create user
def create_user(username: str, email: str) -> int:
    s = Session()
    try:
        stmt = pg_insert(Player).values(
            username=username, email=email, balance=Decimal('0.00')
        ).on_conflict_do_nothing(index_elements=['username'])
        s.execute(stmt); s.commit()
        return s.query(Player).filter_by(username=username).one().player_id
    finally:
        Session.remove()

# 2) Increase balance by random [50,1000]
def increase_balance(player_id: int) -> Decimal:
    amt = Decimal(random.uniform(50, 1000)).quantize(Decimal('0.01'))
    s = Session()
    try:
        with s.begin():
            s.execute(text(
                f"UPDATE {SCHEMA}.player SET balance = balance + :amt WHERE player_id = :pid"
            ), {'amt': amt, 'pid': player_id})
        return amt
    finally:
        Session.remove()

# 3) Generate upcoming block (placeholder)
def generate_block() -> int:
    s = Session()
    try:
        with s.begin():
            last = s.query(Block).order_by(Block.block_id.desc()).first()
            if last and last.actual_char is None:
                raise Exception("Previous block still unresolved")
            res = s.execute(
                pg_insert(Block)
                .values(block_hash=None, actual_char=None)
                .returning(Block.block_id)
            )
            return res.scalar_one()
    finally:
        Session.remove()

# 4) Place a bet on that block
def make_bet(player_id: int, block_id: int, prediction: str, stake: Decimal) -> int:
    s = Session()
    try:
        with s.begin():
            # lock & check balance
            row = s.execute(text(
                f"SELECT balance FROM {SCHEMA}.player WHERE player_id=:pid FOR UPDATE"
            ), {'pid': player_id}).first()
            if not row or row.balance < stake:
                raise Exception("Insufficient balance")
            s.execute(text(
                f"UPDATE {SCHEMA}.player SET balance = balance - :stk WHERE player_id = :pid"
            ), {'stk': stake, 'pid': player_id})

            # verify block is upcoming
            blk = s.query(Block).get(block_id)
            if not blk or blk.actual_char is not None:
                raise Exception("Block not open for betting")

            b = Bet(
                player_id=player_id, block_id=block_id,
                prediction=prediction, stake=stake
            )
            s.add(b)
        return b.bet_id
    finally:
        Session.remove()

# 5a) Generate real hash for a given block
def generate_block_hash(block_id: int) -> str:
    h = uuid.uuid4().hex
    actual = h[-1]
    s = Session()
    try:
        with s.begin():
            s.execute(text(
                f"UPDATE {SCHEMA}.block SET block_hash=:h, actual_char=:c WHERE block_id=:bid"
            ), {'h': h, 'c': actual, 'bid': block_id})
        return h
    finally:
        Session.remove()

# 5b) Resolve **all** bets on any blocks that now have a hash
def resolve_all_bets() -> int:
    s = Session()
    try:
        with s.begin():
            res = s.execute(text(f"""
                UPDATE {SCHEMA}.bet AS bt
                   SET resolved = TRUE,
                       is_win = (bt.prediction = b.actual_char)
                  FROM {SCHEMA}.block AS b
                 WHERE bt.block_id = b.block_id
                   AND b.actual_char IS NOT NULL
                   AND bt.resolved = FALSE
            """))
            return res.rowcount
    finally:
        Session.remove()