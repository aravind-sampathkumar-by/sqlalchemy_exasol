# import all SQLAlchemy tests for this dialect
import pytest

from sqlalchemy.testing.suite import *  # noqa: F403, F401
from sqlalchemy.testing.suite import DifficultParametersTest as _DifficultParametersTest
from sqlalchemy.testing import is_false
from sqlalchemy.testing.suite import HasIndexTest as _HasIndexTest
from sqlalchemy.testing.suite import HasTableTest as _HasTableTest
from sqlalchemy.testing.suite import RowCountTest as _RowCountTest
from sqlalchemy.testing.suite import FetchLimitOffsetTest as _FetchLimitOffsetTest
from sqlalchemy.testing.suite import LongNameBlowoutTest as _LongNameBlowoutTest
from sqlalchemy.testing.suite import CompoundSelectTest as _CompoundSelectTest
from sqlalchemy.testing.suite import ExpandingBoundInTest as _ExpandingBoundInTest
from sqlalchemy.testing.suite import NumericTest as _NumericTest
from sqlalchemy.testing.suite import QuotedNameArgumentTest as _QuotedNameArgumentTest


class CompoundSelectTest(_CompoundSelectTest):

    """Skip this test as EXASOL does not allow EXISTS or IN predicates
    as part of the select list. Skipping is implemented by redefining
    the method as proposed by SQLAlchemy docs for new dialects."""

    def test_null_in_empty_set_is_false(self):
        return


class ExpandingBoundInTest(_ExpandingBoundInTest):

    """Skip this test as EXASOL does not allow EXISTS or IN predicates
    as part of the select list. Skipping is implemented by redefining
    the method as proposed by SQLAlchemy docs for new dialects."""

    def test_null_in_empty_set_is_false(self):
        return


class NumericTest(_NumericTest):

    """FIXME: test skipped to allow upgrading to SQLAlchemy 1.3.x due
    to vulnerability in 1.2.x. Need to understand reason for this.
    Hypothesis is that the data type is not correctly coerced between
    EXASOL and pyodbc."""

    def test_decimal_coerce_round_trip(self):
        return


class QuotedNameArgumentTest(_QuotedNameArgumentTest):

    """This suite was added to SQLAlchemy 1.3.19 on July 2020 to address
    issues in other dialects related to object names that contain quotes
    and double quotes. Since this feature is not relevant to the
    Exasol dialect, the entire suite will be skipped. More info on fix:
    https://github.com/sqlalchemy/sqlalchemy/issues/5456"""

    @pytest.mark.skip()
    def test_get_table_options(self, name):
        return

    @pytest.mark.skip()
    def test_get_view_definition(self, name):
        return

    @pytest.mark.skip()
    def test_get_columns(self, name):
        return

    @pytest.mark.skip()
    def test_get_pk_constraint(self, name):
        return

    @pytest.mark.skip()
    def test_get_foreign_keys(self, name):
        return

    @pytest.mark.skip()
    def test_get_indexes(self, name):
        return

    @pytest.mark.skip()
    def test_get_unique_constraints(self, name):
        return

    @pytest.mark.skip()
    def test_get_table_comment(self, name):
        return

    @pytest.mark.skip()
    def test_get_check_constraints(self, name):
        return


class FetchLimitOffsetTest(_FetchLimitOffsetTest):
    @pytest.mark.skip()
    def test_expr_offset(self):
        return


class LongNameBlowoutTest(_LongNameBlowoutTest):
    @pytest.mark.skip()
    @testing.combinations(
        ("fk",),
        ("pk",),
        ("ix",),
        ("ck", testing.requires.check_constraint_reflection.as_skips()),
        ("uq", testing.requires.unique_constraint_reflection.as_skips()),
        argnames="type_",
    )
    def test_long_convention_name(self, type_, metadata, connection):
        return


class RowCountTest(_RowCountTest):
    @testing.requires.sane_multi_rowcount
    def test_multi_update_rowcount(self, connection):
        employees_table = self.tables.employees
        stmt = (
            employees_table.update()
            .where(
                employees_table.c.name.in_(
                    [
                        "Bob",
                        "Cynthia",
                        "nonexistent",
                    ]
                )
            )
            .values(department="C")
        )

        r = connection.execute(stmt)
        eq_(r.rowcount, 2)

    @testing.requires.sane_multi_rowcount
    def test_multi_delete_rowcount(self, connection):
        employees_table = self.tables.employees

        stmt = employees_table.delete().where(
            employees_table.c.name.in_(
                [
                    "Bob",
                    "Cynthia",
                    "nonexistent",
                ]
            )
        )

        r = connection.execute(stmt)
        eq_(r.rowcount, 2)


class HasTableTest(_HasTableTest):
    pass

    def test_has_table(self):
        with config.db.begin() as conn:
            is_true(config.db.dialect.has_table(conn, "test_table"))
            is_true(config.db.dialect.has_table(conn, "test_table_s"))
            is_false(config.db.dialect.has_table(conn, "nonexistent_table"))

    @testing.requires.schemas
    def test_has_table_schema(self):
        with config.db.begin() as conn:
            is_true(
                config.db.dialect.has_table(
                    conn, "test_table", schema=config.test_schema
                )
            )
            is_true(
                config.db.dialect.has_table(
                    conn, "test_table_s", schema=config.test_schema
                )
            )
            is_false(
                config.db.dialect.has_table(
                    conn, "nonexistent_table", schema=config.test_schema
                )
            )


class HasIndexTest(_HasIndexTest):
    @pytest.mark.skip()
    def test_has_index(self):
        return

    @pytest.mark.skip()
    def test_has_index_schema(self):
        return


class DifficultParametersTest(_DifficultParametersTest):
    __backend__ = True

    @testing.combinations(
        ("boring",),
        ("per cent",),
        ("per % cent",),
        ("%percent",),
        ("par(ens)",),
        ("percent%(ens)yah",),
        ("col:ons",),
        ("more :: %colons%",),
        ("/slashes/",),
        ("more/slashes",),
        ("1param",),
        ("1col:on",),
        argnames="name",
    )
    def test_round_trip(self, name, connection, metadata):
        if name != "q?marks":
            return
        t = Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(name, String(50), nullable=False),
        )

        # table is created
        t.create(connection)

        # automatic param generated by insert
        connection.execute(t.insert().values({"id": 1, name: "some name"}))

        # automatic param generated by criteria, plus selecting the column
        stmt = select(t.c[name]).where(t.c[name] == "some name")

        eq_(connection.scalar(stmt), "some name")

        # use the name in a param explicitly
        stmt = select(t.c[name]).where(t.c[name] == bindparam(name))

        row = connection.execute(stmt, {name: "some name"}).first()

        # name works as the key from cursor.description
        eq_(row._mapping[name], "some name")
