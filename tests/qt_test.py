import sys
sys.path.append("src")
from rsql.qt import *

db = Database(":memory:")
t = db.table("t", text=str, checked=bool)
t.insert(text="Hello", checked=False)
active_count = t.where(checked=0).count()
a = App(sys.argv)
t2 = Table(t, row_fn=lambda row: [table_widget_item(checked=row.edit_checked)], header=["Checked"])
w = MainWindow("todos", t2)
assert active_count.value == 1
t2.itemAt(0, 0).setCheckState(Qt.CheckState.Checked)
assert active_count.value == 0
t2.itemAt(0, 0).setCheckState(Qt.CheckState.Unchecked)
assert active_count.value == 1
