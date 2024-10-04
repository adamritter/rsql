import sys
sys.path.append("src")
from rsql.qt import *

db = Database("dbs/todo_qt_example.db")
t = db.table("t", text=str, checked=bool)
active_count = t.where(checked=0).count()
todo_input = Input(placeholder="What needs to be done?", onenter=lambda: (t.insert(text=todo_input.text(), checked=False), todo_input.setText("")))
where = t.where()
radio_all = RadioButton("All", onclick=lambda: where.set_filter(), checked=True)
radio_active = RadioButton("Active", onclick=lambda: where.set_filter(checked=False))
radio_completed = RadioButton("Completed", onclick=lambda: where.set_filter(checked=True))
group = ButtonGroup(radio_all, radio_active, radio_completed)

def row_fn(row):
    return [table_widget_item(row.edit_text),
            table_widget_item(checked=row.edit_checked),
            Button("x", onclick=row.delete)]

t2 = Table(where, row_fn=row_fn, header=["Text", "Checked", "Delete"])
vbt = VBoxSorted(where.sort(),
                  row_fn=lambda row: HBox(row.edit_text, CheckBox(checked=row.edit_checked), Button("x", onclick=row.delete)))

w = MainWindow("todos",
        VBox(HBox(CheckBox(checked=active_count.map(lambda value: value == 0),
                            onclick=lambda checked: t.update({}, checked=checked)),
                    todo_input),
            t2,
            vbt,
            HBox(Label(active_count.map(lambda x: "1 item left" if x == 1 else f"{x} items left")),
                 radio_all,
                 radio_active, 
                 radio_completed,
                 show_if(t.where(checked=1).count(),
                         Button("Clear completed", onclick=lambda: t.delete(checked=1)))), 
            Button("Quit", onclick=exit),
            ))

exec_app()
