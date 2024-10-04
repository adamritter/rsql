# TODO: show database tables
# To make qt_editor edit itself, it needs to derive all state (except maybe focus / selection) from the database
# also a query editor is needed (maybe simple, but it must be able to run arbitrary queries and display the results)
import sys
sys.path.append("src")
from rsql.qt import *

def ReactiveWidget(table, cb):
    widget = SingleWidgetContainer()
    table.onvalue(lambda new: widget.setWidget(cb(new)))
    return widget

db = Database("widgets.db")
widgets_table = db.table(
    "widgets", 
    parent_id=int,      # Parent widget ID (-1 for root)
    name=str,           # Name of the widget
    widget_type=str,    # Type of the widget: 'Label', 'VBox', 'HBox'
    text=str            # Text content (only for 'Label' type)
)
# add root widget if not exists
if not widgets_table.where(parent_id=-1).fetchall():
    widgets_table.insert(parent_id=-1, name="Root", widget_type="VBox")

selections_table = db.table("selections", temp=True, widget_id=int, tname=str, query_id=int)
selections_table.insert(widget_id=None, tname="widgets", query_id=None)

app = App(sys.argv)

widgets_table_view = Table(
    widgets_table,
    row_fn=lambda row: [
        row.edit_id,
        row.edit_parent_id,
        row.edit_name,
        row.edit_widget_type,
        row.edit_text,
        Button("Delete", onclick=row.delete)
    ],
    header=["ID", "Parent ID", "Name", "Widget Type", "Text", "Actions"]
)

# Recursive rendering of widgets
def render_widget(widget_row):
    if widget_row.widget_type == "Label":
        return Label(widget_row.view_text)
    else:
        child_rows = widgets_table.where(parent_id=widget_row.id)
        if widget_row.widget_type == "VBox":
            return VBoxSorted(child_rows.sort(), row_fn=render_widget)
        elif widget_row.widget_type == "HBox":
            return HBoxSorted(child_rows.sort(), row_fn=render_widget)

tname = selections_table.select(tname=True).only().map(lambda tname_row: tname_row['tname'])
right = VBox(
    show_if(tname, 
            ReactiveWidget(
                tname,
                lambda tname_value: Table(db.table(tname_value)) if tname_value else None
            )),
    show_unless(tname, VBoxSorted(widgets_table.where(parent_id=-1).sort(), row_fn=render_widget))
)

window = None

def tree_widget_row_fn(row):
    text = row.widget_type
    if row.name:
        text += f" ({row.name})"
    if row.widget_type == "Label":
        text += f": {row.text}"
    return HBox(Label(text))

item_editor = SingleWidgetContainer()
edit_item_id = None
def edit_item(row):
    global edit_item_id
    if not row:
        edit_item_id = None
        item_editor.setWidget(None)
        return
    selections_table.update({}, widget_id=row.id, tname=None)
    if row.__updating__ and rsql.LOCAL_CHANGE:
        print("edit item skipped due to updating")
        return
    r = [Label(row.widget_type), HBox("name", row.edit_name)]
    if row.widget_type == "Label":
        r.append(HBox("text", row.edit_text))
    if row.widget_type == "VBox" or row.widget_type == "HBox":
        r.append(HBox(Button("Add label", onclick=lambda: widgets_table.insert(parent_id=row.id, widget_type="Label")),
                      Button("Add VBox", onclick=lambda: widgets_table.insert(parent_id=row.id, widget_type="VBox")),
                      Button("Add HBox", onclick=lambda: widgets_table.insert(parent_id=row.id, widget_type="HBox"))))
    item_editor.setWidget(VBox(
        *r,
        Button("Delete", onclick=row.delete),
        Button("Show", onclick=lambda: print_widget_hierarchy(window))
    ), keep_focus=(edit_item_id == row.id))
    edit_item_id = row.id

def show_table(table):
    item_editor.setWidget(Table(table))
    print("showing table", table.name)


def show_table_button(table):
    print("show_table_button", table.name)  
    return Button(f"{table.name}({str(table.columns)})", onclick=lambda: selections_table.update({}, tname=table.name))


db_editor = VBox(*[show_table_button(t) for t in db.tables.values()])

left = VBox(
    TreeWidget(widgets_table, row_fn=tree_widget_row_fn,
               onselect=edit_item, onenter=edit_item),
    item_editor, db_editor)

main_layout = HBox(left, right)

window = MainWindow("Dynamic Widget Application", main_layout, height=1028)
print("APP", APP)
exec_app()