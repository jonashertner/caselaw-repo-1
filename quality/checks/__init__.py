"""Quality-check implementations.

Each module in this package defines one or more `check_*` functions
that take a read-only `sqlite3.Connection` to `output/decisions.db`
and return either a `CheckResult` or an iterable of them.

The `runner` discovers them automatically — adding a new check
requires no registration. The dotted name in the report is
`<module>.<function_name_without_check_prefix>`.

Example:

    # quality/checks/dates.py
    def check_future_dates(conn, **ctx):
        n = conn.execute(
            "SELECT COUNT(*) FROM decisions WHERE decision_date > ?",
            (today_plus_30(),)
        ).fetchone()[0]
        return CheckResult(
            name="dates.future_dates",
            severity=Severity.CRITICAL,
            passed=(n <= 50),
            metric_value=n,
            threshold=50,
            message=f"{n} decisions dated > today+30d",
        )
"""
