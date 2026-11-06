"""Allow crypto instruments (additive): Binance BTCUSDT candles (UPDATE2, 2026-11-04).

Only widens the ``instruments.asset_class`` check; existing rows are unaffected.

Revision ID: 0008_crypto_asset_class
Revises: 0007_partition_grants
"""

from alembic import op

from marketagg.db.migrate import require_disposable_for_downgrade

revision = "0008_crypto_asset_class"
down_revision = "0007_partition_grants"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE instruments DROP CONSTRAINT instruments_asset_class_check")
    op.execute("ALTER TABLE instruments ADD CONSTRAINT instruments_asset_class_check "
               "CHECK (asset_class = ANY (ARRAY['equity'::text, 'etf'::text, 'crypto'::text]))")


def downgrade() -> None:
    require_disposable_for_downgrade(op.get_bind())
    op.execute("ALTER TABLE instruments DROP CONSTRAINT instruments_asset_class_check")
    op.execute("ALTER TABLE instruments ADD CONSTRAINT instruments_asset_class_check "
               "CHECK (asset_class = ANY (ARRAY['equity'::text, 'etf'::text]))")
