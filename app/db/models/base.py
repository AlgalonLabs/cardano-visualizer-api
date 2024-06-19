from sqlalchemy import create_engine, Column, Integer, BigInteger, String, ForeignKey, Numeric, Boolean, DateTime, \
    LargeBinary, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()


class Block(Base):
    __tablename__ = 'block'

    id = Column(BigInteger, primary_key=True)
    hash = Column(LargeBinary, unique=True, nullable=False)
    epoch_no = Column(Integer)
    slot_no = Column(BigInteger)
    epoch_slot_no = Column(Integer)
    block_no = Column(Integer)
    previous_id = Column(BigInteger, ForeignKey('block.id'))
    slot_leader_id = Column(BigInteger, nullable=False)
    size = Column(Integer, nullable=False)
    time = Column(DateTime, nullable=False)
    tx_count = Column(BigInteger, nullable=False)
    proto_major = Column(Integer, nullable=False)
    proto_minor = Column(Integer, nullable=False)
    vrf_key = Column(String)
    op_cert = Column(LargeBinary)
    op_cert_counter = Column(BigInteger)

    previous_block = relationship('Block', remote_side=[id], backref='next_blocks')

    __table_args__ = (
        Index('idx_block_block_no', 'block_no'),
        Index('idx_block_epoch_no', 'epoch_no'),
        Index('idx_block_previous_id', 'previous_id'),
        Index('idx_block_slot_leader_id', 'slot_leader_id'),
        Index('idx_block_slot_no', 'slot_no'),
        Index('idx_block_time', 'time')
    )


class StakeAddress(Base):
    __tablename__ = 'stake_address'

    id = Column(BigInteger, primary_key=True)
    hash_raw = Column(LargeBinary, unique=True, nullable=False)
    view = Column(String, nullable=False)
    script_hash = Column(LargeBinary)

    __table_args__ = (
        Index('idx_stake_address_hash_raw', 'hash_raw'),
        Index('idx_stake_address_view', 'view', postgresql_using='hash')
    )


class MultiAsset(Base):
    __tablename__ = 'multi_asset'

    id = Column(BigInteger, primary_key=True)
    policy = Column(LargeBinary, nullable=False)
    name = Column(LargeBinary, nullable=False)
    fingerprint = Column(String, nullable=False)

    __table_args__ = (
        Index('unique_multi_asset', 'policy', 'name', unique=True),
    )


class TxOut(Base):
    __tablename__ = 'tx_out'

    id = Column(BigInteger, primary_key=True)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)
    index = Column(Integer, nullable=False)
    address = Column(String, nullable=False)
    address_has_script = Column(Boolean, nullable=False)
    payment_cred = Column(LargeBinary)
    stake_address_id = Column(BigInteger, ForeignKey('stake_address.id'))
    value = Column(Numeric(20, 0), nullable=False)
    data_hash = Column(LargeBinary)
    inline_datum_id = Column(BigInteger)
    reference_script_id = Column(BigInteger)

    stake_address = relationship('StakeAddress')

    __table_args__ = (
        Index('idx_tx_out_payment_cred', 'payment_cred'),
        Index('idx_tx_out_stake_address_id', 'stake_address_id'),
        Index('idx_tx_out_tx_id', 'tx_id'),
        Index('tx_out_inline_datum_id_idx', 'inline_datum_id'),
        Index('tx_out_reference_script_id_idx', 'reference_script_id')
    )


class Transaction(Base):
    __tablename__ = 'tx'

    id = Column(BigInteger, primary_key=True)
    hash = Column(LargeBinary, unique=True, nullable=False)
    block_id = Column(BigInteger, ForeignKey('block.id'), nullable=False)
    block_index = Column(Integer, nullable=False)
    out_sum = Column(Numeric(20, 0), nullable=False)
    fee = Column(Numeric(20, 0), nullable=False)
    deposit = Column(BigInteger)
    size = Column(Integer, nullable=False)
    invalid_before = Column(Numeric(20, 0))
    invalid_hereafter = Column(Numeric(20, 0))
    valid_contract = Column(Boolean, nullable=False)
    script_size = Column(Integer, nullable=False)

    block = relationship('Block')

    __table_args__ = (
        Index('idx_tx_block_id', 'block_id'),
    )


class MATxOut(Base):
    __tablename__ = 'ma_tx_out'

    id = Column(BigInteger, primary_key=True)
    quantity = Column(Numeric(20, 0), nullable=False)
    tx_out_id = Column(BigInteger, ForeignKey('tx_out.id'), nullable=False)
    ident = Column(BigInteger, ForeignKey('multi_asset.id'), nullable=False)

    tx_out = relationship('TxOut')
    multi_asset = relationship('MultiAsset')

    __table_args__ = (
        Index('idx_ma_tx_out_tx_out_id', 'tx_out_id'),
    )


class MATxMint(Base):
    __tablename__ = 'ma_tx_mint'

    id = Column(BigInteger, primary_key=True)
    quantity = Column(Numeric(20, 0), nullable=False)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)
    ident = Column(BigInteger, ForeignKey('multi_asset.id'), nullable=False)

    tx = relationship('Transaction')
    multi_asset = relationship('MultiAsset')

    __table_args__ = (
        Index('idx_ma_tx_mint_tx_id', 'tx_id'),
    )


# Create engine and session
engine = create_engine('postgresql://user:password@localhost/cardano_db')
Session = sessionmaker(bind=engine)
session = Session()

# Create all tables
Base.metadata.create_all(engine)
