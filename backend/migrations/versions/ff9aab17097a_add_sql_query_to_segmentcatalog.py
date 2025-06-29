"""Add sql_query to SegmentCatalog

Revision ID: ff9aab17097a
Revises: 
Create Date: 2025-06-23 21:37:10.572129

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ff9aab17097a'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('segment_catalog', schema=None) as batch_op:
        batch_op.add_column(sa.Column('sql_query', sa.Text(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('segment_catalog', schema=None) as batch_op:
        batch_op.drop_column('sql_query')
    # ### end Alembic commands ###
