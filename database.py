"""
database.py — SQLite database setup with SQLAlchemy.
"""
from sqlalchemy import Column, Integer, String, Float, Text, DateTime, ForeignKey, create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from datetime import datetime, timezone

DATABASE_URL = "sqlite:///./accounting.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Client(Base):
    __tablename__ = "clients"
    client_id  = Column(String, primary_key=True, index=True)
    settore    = Column(String, nullable=True)   # es: "medico", "avvocato"
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    reports    = relationship("FinancialReport", back_populates="client")


class FinancialReport(Base):
    __tablename__ = "financial_reports"
    id                  = Column(Integer, primary_key=True, autoincrement=True)
    client_id           = Column(String, ForeignKey("clients.client_id"), index=True)
    periodo             = Column(String, index=True)
    settore             = Column(String, nullable=True)
    ricavi              = Column(Float)
    costi               = Column(Float)
    margine             = Column(Float)
    margine_percentuale = Column(Float)
    commento_ai         = Column(Text, nullable=True)
    created_at          = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    client   = relationship("Client", back_populates="reports")
    accounts = relationship("AccountEntry", back_populates="report", cascade="all, delete-orphan")


class AccountEntry(Base):
    __tablename__ = "account_entries"
    id                  = Column(Integer, primary_key=True, autoincrement=True)
    report_id           = Column(Integer, ForeignKey("financial_reports.id"), index=True)
    codice              = Column(String, nullable=True)
    descrizione         = Column(String)
    tipo                = Column(String)
    incassi             = Column(Float, default=0.0)
    pagamenti           = Column(Float, default=0.0)
    rettifiche          = Column(Float, default=0.0)
    reddito_rettificato = Column(Float, nullable=True)
    report = relationship("FinancialReport", back_populates="accounts")


def init_db():
    Base.metadata.create_all(bind=engine)
    _migrate()

def _migrate():
    """Aggiunge colonne mancanti senza distruggere i dati esistenti."""
    with engine.connect() as conn:
        # Legge le colonne esistenti nella tabella clients
        result = conn.execute(text("PRAGMA table_info(clients)"))
        client_cols = {row[1] for row in result}

        if "settore" not in client_cols:
            conn.execute(text("ALTER TABLE clients ADD COLUMN settore VARCHAR"))
            conn.commit()

        # Legge le colonne esistenti nella tabella financial_reports
        result = conn.execute(text("PRAGMA table_info(financial_reports)"))
        report_cols = {row[1] for row in result}

        if "settore" not in report_cols:
            conn.execute(text("ALTER TABLE financial_reports ADD COLUMN settore VARCHAR"))
            conn.commit()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
