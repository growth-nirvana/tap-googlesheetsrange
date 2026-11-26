"""skip_footer: the block of total / note / copyright rows under a report's data is left out."""

from sheets_table import Read, SheetSpec, build_table

HEADER = ["Opportunity ID", "Account Name", "Close Date", "Amount", "Stage"]
BODY = [
    ["006FX0000000001AAA", "Acme Example Co", "1/15/2026", "$1,234.50", "Prospecting"],
    ["006FX0000000005AAA", "Hooli Fixture", None, None, None],
    ["006FX0000000008AAA", "Wayne Example", "7/20/2026", "-$250.00", "Closed"],
]
FOOTER = [
    [],
    ["Total", "", "", "$984.50", ""],
    ["Confidential Information - Do Not Distribute"],
    ["Copyright © 2000-2026 salesforce.com, inc. All rights reserved."],
]


def table(values, skip_footer=True):
    spec = SheetSpec.from_config(
        {"stream_name": "x", "spreadsheet_url": "1hNMlbLOwhkol0isTS2Zg7gpppVOUO5xBScZR3ZM8fcs",
         "header_row": "auto", "skip_blank_rows": True, "skip_footer": skip_footer}
    )
    return build_table(spec, Read(values, 1, 0), normalize=True, sheet_rows=True)


def test_a_report_footer_is_left_out_and_listed():
    t = table([HEADER, *BODY, *FOOTER])
    assert [r["opportunity_id"] for r in t.rows] == [b[0] for b in BODY]
    assert [f["sheet_row"] for f in t.footer] == [5, 6, 7, 8]
    assert t.footer[1]["text"] == "Total $984.50"
    assert t.footer_kept is None


def test_off_by_default_everything_loads():
    t = table([HEADER, *BODY, *FOOTER], skip_footer=False)
    assert len(t.rows) == 6 and t.footer == []
    spec = SheetSpec.from_config({"stream_name": "x", "spreadsheet_url": "1hNMlbLOwhkol0isTS2Zg7gpppVOUO5xBScZR3ZM8fcs"})
    assert spec.skip_footer is False


def test_a_short_last_data_row_still_loads():
    t = table([HEADER, *BODY, ["006FX0000000009AAA", "Last Short Co"]])
    assert len(t.rows) == 4 and t.footer == []


def test_a_short_row_in_the_middle_always_loads():
    t = table([HEADER, BODY[0], ["A note in the middle"], *BODY[1:]])
    assert len(t.rows) == 4


def test_nothing_is_left_out_when_the_footer_would_outweigh_the_data():
    t = table([HEADER, BODY[0], ["note one"], ["note two"]])
    assert len(t.rows) == 3 and t.footer == []
    assert "more than the 1 rows above them" in t.footer_kept


def test_nothing_is_left_out_past_ten_footer_rows():
    many = [[f"Note number {i} about this report"] for i in range(11)]
    t = table([HEADER, *(BODY * 10), *many])
    assert len(t.rows) == 41 and "more than 10" in t.footer_kept


def counted(values, footer_rows):
    spec = SheetSpec.from_config(
        {"stream_name": "x", "spreadsheet_url": "1hNMlbLOwhkol0isTS2Zg7gpppVOUO5xBScZR3ZM8fcs",
         "header_row": "auto", "skip_blank_rows": True, "footer_rows": footer_rows}
    )
    return build_table(spec, Read(values, 1, 0), normalize=True, sheet_rows=True)


def test_a_count_leaves_out_exactly_that_many_rows_at_the_bottom():
    # Keep the Total row: leave out only the disclaimer and the copyright.
    t = counted([HEADER, *BODY, *FOOTER], 2)
    assert [f["sheet_row"] for f in t.footer] == [7, 8]
    assert [r["opportunity_id"] for r in t.rows][-1] == "Total"


def test_auto_and_the_old_switch_find_the_footer():
    assert [f["sheet_row"] for f in counted([HEADER, *BODY, *FOOTER], "auto").footer] == [5, 6, 7, 8]
    assert SheetSpec.from_config({"stream_name": "x", "spreadsheet_url": "u" * 30, "skip_footer": True}).footer_rows == "auto"
    assert counted([HEADER, *BODY, *FOOTER], 0).footer == []


def test_a_count_as_big_as_the_table_leaves_nothing_out():
    t = counted([HEADER, *BODY], 3)
    assert len(t.rows) == 3 and t.footer == [] and "not more than 3" in t.footer_kept
