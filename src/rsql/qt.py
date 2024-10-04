import sys
from PyQt6.QtWidgets import (QApplication, QMainWindow, QPushButton, QVBoxLayout, QLabel, QWidget,
                             QLineEdit, QFrame, QGridLayout, QScrollArea, QDialog, QFileDialog,
                             QCheckBox, QHBoxLayout, QListWidget, QComboBox, QTableWidget, 
                             QTableWidgetItem, QButtonGroup, QRadioButton, QMessageBox, QTreeWidget, QTreeWidgetItem)
from PyQt6.QtGui import QPixmap
from PyQt6.QtCore import QTimer, Qt
import rsql, importlib, os, sys
from rsql import Database  # Reexport

def reload(): importlib.reload(sys.modules[__name__])

# TODO: Set dynamic properties
# TODO: Totally generic constructor for all widgets
# TODO: Generic callbacks for all widgets
# TODO: Generic event handling for all widgets
# TODO: Generic layout handling for all widgets
# TODO: Generic signal/slot handling for all widgets

MessageBox = QMessageBox
APP = None

def print_widget_hierarchy(w, indent=0):
    if isinstance(w, QLabel):
        print(" "*indent, "Label:" ,w.text())
    elif isinstance(w, QLineEdit):
        print(" "*indent, "QLineEdit:" ,w.text(), w.hasFocus(), w.cursorPosition())
    elif isinstance(w, QTreeWidgetItem):
        print(" "*indent, "QTreeWidgetItem:")
        for i in range(w.columnCount()):
            print(" "*indent, "  ", w.text(i))
    else:
        print("  "*indent, w, w.hasFocus())
    if hasattr(w, "layout") or isinstance(w, SingleWidgetContainer):
        layout = w.layout
        if callable(layout):
            layout = layout()
        if layout:
            print("  "*indent, "->", layout)
            for i in range(layout.count()):
                print_widget_hierarchy(layout.itemAt(i).widget(), indent+1)

def store_focus(w):
    if isinstance(w, QLineEdit):
        return [QLineEdit, w.hasFocus(), w.cursorPosition()] if w.hasFocus() else None
    elif isinstance(w, QCheckBox):
        return [QCheckBox, w.hasFocus(), w.isChecked()] if w.hasFocus() else None
    elif w.hasFocus():
        return [type(w)]
    elif hasattr(w, "layout") or isinstance(w, SingleWidgetContainer):
        layout = w.layout
        if callable(layout):
            layout = layout()
        if layout:
            for i in range(layout.count()):
                focus = store_focus(layout.itemAt(i).widget())
                if focus:
                    return [type(w), i, focus]
    return None

def restore_focus(w, focus):
    if focus is None or type(w) != focus[0]:
        return
    if isinstance(w, QLineEdit):
        w.setCursorPosition(focus[2])
        w.setFocus()
    elif isinstance(w, QCheckBox):
        w.setChecked(focus[2])
        w.setFocus()
    elif len(focus) == 3 and (hasattr(w, "layout") or isinstance(w, SingleWidgetContainer)):
        layout = w.layout
        if callable(layout):
            layout = layout()
        if layout and focus[1] < layout.count():
            restore_focus(layout.itemAt(focus[1]).widget(), focus[2])

def print_props(w):
    meta = w.metaObject()
    for i in range(meta.propertyCount()):
        prop = meta.property(i)
        print(f"{prop.name()}: {prop.typeName()} = {w.property(prop.name())} / {prop.read(w)}, writable: {prop.isWritable()}")
    for i in range(w.metaObject().methodCount()):
        method = w.metaObject().method(i)
        print(f"{method.methodType()}: {method.name().data().decode()}: {method.typeName()} / {method.methodSignature().data().decode()}")
    print("Done")

def onValue(value, fn):
    if isinstance(value, rsql.Value):
        value.onvalue(fn)
    else:
        fn(value)
IN_EVENT_LOOP = False

def reload_and_eval_main():
    # close main window (how to find all windows?)
    ws = QApplication.allWidgets()

    # Get the path of the main script
    main_file = sys.argv[0]
    
    # Load the module specification
    spec = importlib.util.spec_from_file_location("__main__", main_file)
    
    # Create a new module based on the spec
    module = importlib.util.module_from_spec(spec)
    
    # Execute the module
    try:
        spec.loader.exec_module(module)
        # Now get the main windows
        main_windows = []
        for w in ws:
            if isinstance(w, QMainWindow):
                main_windows.append(w)
        print("main_windows", len(main_windows))
        if False and len(main_windows) == 1:
            old_main_window = main_windows[0]
            ws2 = QApplication.allWidgets()
            new_main_windows = []
            for w in ws2:
                if w != old_main_window and isinstance(w, QMainWindow):
                    new_main_windows.append(w)
            print("new_main_windows", len(new_main_windows))
            if len(new_main_windows) == 1:
                new_main_window = new_main_windows[0]
                # move content to old main window
                new_main_window.centralWidget().setParent(old_main_window)
                old_main_window.setWindowTitle(new_main_window.windowTitle())
                old_main_window.show()
                old_main_window.activateWindow()
                
                print("closing new main window")
                new_main_window.setCentralWidget(None)
                new_main_window.close()

                print("closed new main window")
                return
        for w in ws:
            if hasattr(w, "close"):
                w.close()
    except Exception as e:
        print(f"Error executing module: {e}")

def exec_app(live=True):
    global APP, IN_EVENT_LOOP
    if IN_EVENT_LOOP:
        print("Reloaded")
        return
    IN_EVENT_LOOP = True
    # Get main file
    main_file = sys.argv[0]
    # watch for file changes
    if live:
        import hashlib
        def get_file_hash(filename):
            with open(filename, "rb") as f:
                return hashlib.md5(f.read()).hexdigest()
        last_hash = get_file_hash(main_file)
        timer = QTimer()
        def check_file_change():
            nonlocal last_hash
            current_hash = get_file_hash(main_file)
            if current_hash != last_hash:
                last_hash = current_hash
                reload_and_eval_main()
        timer.timeout.connect(check_file_change)
        timer.start(100)  # Check every 100 ms
    sys.exit(APP.exec())

def widget(klass, **kwargs):
    global APP
    if APP is None:
        APP = QApplication(sys.argv)
    w = klass()
    for prop, value in kwargs.items():
        if prop.startswith("on"):
            if not value:
                continue
            if prop == "onclick":
                prop = "clicked"
            elif prop == "onenter":
                prop = "returnPressed"
            signal = getattr(w, prop.replace("on", ""))
            signal.connect(value)
        else:
            while "_" in prop:
                index = prop.index("_")
                prop = prop[0:index] + prop[index+1:].capitalize()
            onValue(value, lambda new: w.setProperty(prop, new))
    return w

def MainWindow(window_title=None, central_widget=None, left=0, top=0, width=600, height=800, show=True, **kwargs):
    win = widget(QMainWindow, window_title=window_title, width=width, height=height, 
                 central_widget=central_widget, **kwargs)
    if width and height:
        win.setGeometry(left, top, width, height)
    if central_widget:
        win.setCentralWidget(central_widget)
    if show:
        win.show()
    return win

def lazystr(s):
    if isinstance(s, rsql.Value):
        return s
    elif isinstance(s, rsql.EditableRowItem):
        return s.value
    else:
        return str(s)

def Label(text, **kwargs):
    return widget(QLabel, text=lazystr(text), **kwargs)

def Input(text=None, placeholder_text=None, onenter=None, **kwargs):
    entry = widget(QLineEdit, text=text, placeholder_text=placeholder_text, onenter=onenter, **kwargs)
    return entry

def to_widget(obj):
    if isinstance(obj, QWidget):
        return obj
    elif isinstance(obj, rsql.Value):
        label = QLabel(str(obj.value))
        obj.onchange(lambda new: label.setText(str(new)))
        return label
    elif isinstance(obj, rsql.EditableRowItem):
        entry = QLineEdit()
        # str(obj.value))
        obj.onvalue(lambda value: entry.setText(str(value)))
        entry.editingFinished.connect(lambda: obj.set(entry.text()))
        return entry
    elif isinstance(obj, str):
        return QLabel(obj)
    elif isinstance(obj, int):
        return QLabel(str(obj))
    elif isinstance(obj, bool):
        return QCheckBox(str(obj))
    elif isinstance(obj, float):
        return QLabel(str(obj))
    else:
        raise ValueError(f"Cannot convert {type(obj)} to QWidget")

def reset_layout(layout, sql_view, row_fn):
    global WIDGET_TO_ROW
    for i in reversed(range(layout.count())):
        removeLater(layout, i)
    for row in sql_view:
        layout_on_insert(layout, layout.count(), row, row_fn)

WIDGET_TO_ROW = {}

def layout_on_insert(layout, index, row, row_fn):
    global WIDGET_TO_ROW
    widget = to_widget(row_fn(row))
    WIDGET_TO_ROW[widget] = row
    layout.insertWidget(index, widget)

def on_update_box_sorted(oind, nind, old, new, layout, row_fn):
    global WIDGET_TO_ROW
    widget = layout.itemAt(oind).widget()
    if widget in WIDGET_TO_ROW:
        row = WIDGET_TO_ROW[widget]
        if row.__updating__:
            return
    if old.__updating__ or new.__updating__:
        return
    removeLater(layout, oind)
    layout_on_insert(layout, nind, new, row_fn)

def VBoxSorted(sql_view, row_fn, **kwargs):
    w = widget(QWidget)
    layout = QVBoxLayout()
    cbs = []
    for row in sql_view:
        layout_on_insert(layout, layout.count(), row, row_fn)
    w.setLayout(layout)
    cbs.append(sql_view.on_insert(lambda index, row: layout_on_insert(layout, index, row, row_fn)))
    cbs.append(sql_view.on_delete(lambda index, _: removeLater(layout, index)))
    cbs.append(sql_view.on_update(lambda oind, nind, old, new: on_update_box_sorted(oind, nind, old, new, layout, row_fn)))
    cbs.append(sql_view.on_reset(lambda: reset_layout(layout, sql_view, row_fn)))
    w.destroyed.connect(lambda: [cb() for cb in cbs])
    return w

def removeLater(layout, index):
    global WIDGET_TO_ROW
    widget = layout.itemAt(index).widget()
    if widget in WIDGET_TO_ROW:
        del WIDGET_TO_ROW[widget]
    layout.removeWidget(widget)
    widget.deleteLater()

def HBoxSorted(sql_view, row_fn, **kwargs):
    w = widget(QWidget)
    layout = QHBoxLayout()
    for row in sql_view:
        layout_on_insert(layout, layout.count(), row, row_fn)
    w.setLayout(layout)
    cbs = []
    cbs.append(sql_view.on_insert(lambda index, row: layout_on_insert(layout, index, row, row_fn)))
    cbs.append(sql_view.on_delete(lambda index, _: removeLater(layout, index)))
    cbs.append(sql_view.on_update(lambda oind, nind, old, new: on_update_box_sorted(oind, nind, old, new, layout, row_fn)))
    cbs.append(sql_view.on_reset(lambda: reset_layout(layout, sql_view, row_fn)))
    w.destroyed.connect(lambda: [cb() for cb in cbs])
    return w

def VBox(*children, **kwargs):
    w = widget(QWidget)
    layout = QVBoxLayout()
    for child in children:
        layout.addWidget(to_widget(child))
    w.setLayout(layout)
    return w

def HBox(*children, **kwargs):
    w = widget(QWidget)
    layout = QHBoxLayout()
    for child in children:
        layout.addWidget(to_widget(child))
    w.setLayout(layout)
    return w

def ScrollArea(**kwargs):
    return widget(QScrollArea, **kwargs)

def CheckBox(label=None, checked=False, onclick=None, **kwargs):
    checkbox = widget(QCheckBox, label=label, onclick=onclick, **kwargs)
    if isinstance(checked, rsql.Value):
        checked.onvalue(lambda new: checkbox.setChecked(new))
    elif isinstance(checked, rsql.EditableRowItem):
        checkbox.setChecked(checked.value)
        checkbox.stateChanged.connect(lambda state: checked.set(state))
    else:
        checkbox.setChecked(checked)
    return checkbox

def dialog(**kwargs):
    return widget(QDialog, **kwargs)

def file_chooser_dialog(title, parent, action, **kwargs):
    if action == QFileDialog.AcceptOpen:
        return QFileDialog.getOpenFileName(parent, title, **kwargs)
    elif action == QFileDialog.AcceptSave:
        return QFileDialog.getSaveFileName(parent, title, **kwargs)

def list_box(selection_mode, **kwargs):
    list_widget = widget(QListWidget, **kwargs)
    if selection_mode == 'single':
        list_widget.setSelectionMode(QListWidget.SelectionMode.SingleSelection)
    elif selection_mode == 'multiple':
        list_widget.setSelectionMode(QListWidget.SelectionMode.MultiSelection)
    return list_widget

def Button(text, onclick=None, **kwargs):
    return widget(QPushButton, text=text, onclick=onclick, **kwargs)

def App(argv=None):
    if argv is None:
        argv = sys.argv
    r =  QApplication(argv)
    r.timer = interrupt_timer()
    return r

TABLE_DATA = {}
import time
def update_table_row(table, sql_view, old, new, row_fn):
    print("update_table_row", old, new)
    t=time.time()
    global CAN_CHANGE_CELL
    backup = CAN_CHANGE_CELL
    CAN_CHANGE_CELL = False
    global TABLE_DATA
    # find old row
    for i in range(table.rowCount()):
        if TABLE_DATA[table][i] == old:
            items = row_fn(new)
            for j in range(table.columnCount()):
                item = items[j]
                if isinstance(item, rsql.EditableRowItem):
                    item = to_widget(item)
                if isinstance(item, QWidget):
                    table.setCellWidget(i, j, item)
                else:
                    table.setItem(i, j, item)
            TABLE_DATA[table][i] = new
            break
    CAN_CHANGE_CELL = backup
    print("update_table_row time: ", time.time()-t)

def check_table(table, sql_view):
    # All data should match
    table_data = TABLE_DATA[table] if table in TABLE_DATA else []
    if len(table_data) != table.rowCount():
        raise Exception(f"Table data length mismatch: {len(table_data)} != {table.rowCount()}")
    return # can't do general check
    for i in range(table.rowCount()):
        item = table.item(i, 0) # Text
        table_row = table_data[i]
        # print("table_row", table_row)
        if item:
            if item.text() != table_row.text:
                raise Exception(f"Table data mismatch at row {i}: {item.text()} != {table_row.text}")

def insert_table_row(table, sql_view, row, row_fn):
    global CAN_CHANGE_CELL
    backup = CAN_CHANGE_CELL
    CAN_CHANGE_CELL = False
    check_table(table, sql_view)

    table.insertRow(table.rowCount())
    items = row_fn(row)
    for i, item in enumerate(items):
        if isinstance(item, rsql.EditableRowItem):
            item = to_widget(item)
        if isinstance(item, QWidget):
            table.setCellWidget(table.rowCount() - 1, i, item)
        else:
            table.setItem(table.rowCount() - 1, i, item)
    # set custom user data
    global TABLE_DATA
    if not table in TABLE_DATA:
        TABLE_DATA[table] = []
    TABLE_DATA[table].append(row)
    # print("insert_table_row", row)
    # print("table data[table]", TABLE_DATA[table])
    check_table(table, sql_view)
    CAN_CHANGE_CELL = backup

def delete_table_row_dummy(table, sql_view, row):
    pass

def delete_table_row(table, sql_view, row):
    # print("delete_table_row", row)
    global CAN_CHANGE_CELL
    backup = CAN_CHANGE_CELL
    CAN_CHANGE_CELL = False
    global TABLE_DATA
    # print("finding row to delete", row)
    check_table(table, sql_view)
    found = False
    for i in range(table.rowCount()):
        # print("TABLE_DATA[table][i]", TABLE_DATA[table][i])
        if TABLE_DATA[table][i] == row:
            # Before removing the row, delete any cell widgets
            for j in range(table.columnCount()):
                cell_widget = table.cellWidget(i, j)
                if cell_widget:
                    table.removeCellWidget(i, j)
                    cell_widget.deleteLater()
                # Also delete QTableWidgetItem instances
                item = table.item(i, j)
                if item:
                    table.setItem(i, j, None)
                    # No need to delete QTableWidgetItem manually
            table.removeRow(i)
            TABLE_DATA[table] = TABLE_DATA[table][:i] + TABLE_DATA[table][i+1:]
            found = True
            break
    # print(f"delete table data for id {row.id} found: {found}")
    check_table(table, sql_view)
    CAN_CHANGE_CELL = backup


def insert_sorted_table_row(table, index, row, row_fn):
    global CAN_CHANGE_CELL
    backup = CAN_CHANGE_CELL
    CAN_CHANGE_CELL = False
    table.insertRow(index)
    items = row_fn(row)
    for i, item in enumerate(items):
        if isinstance(item, rsql.EditableRowItem):
            item = to_widget(item)
        if isinstance(item, QWidget):
            table.setCellWidget(index, i, item)
        else:
            table.setItem(index, i, item)
    CAN_CHANGE_CELL = backup

def delete_sorted_table_row(table, index):
    global CAN_CHANGE_CELL
    backup = CAN_CHANGE_CELL
    CAN_CHANGE_CELL = False
    # Before removing the row, delete any cell widgets
    for j in range(table.columnCount()):
        cell_widget = table.cellWidget(index, j)
        if cell_widget:
            table.removeCellWidget(index, j)
            cell_widget.deleteLater()
        # Also delete QTableWidgetItem instances
        item = table.item(index, j)
        if item:
            table.setItem(index, j, None)
            # No need to delete QTableWidgetItem manually
    table.removeRow(index)
    CAN_CHANGE_CELL = backup


def update_sorted_table_row(table, oind, nind, nrow, row_fn):
    global CAN_CHANGE_CELL
    backup = CAN_CHANGE_CELL
    CAN_CHANGE_CELL = False
    delete_sorted_table_row(table, oind)
    insert_sorted_table_row(table, nind, nrow, row_fn)
    CAN_CHANGE_CELL = backup

def interrupt_timer():
    timer = QTimer()
    def check_for_interrupt():
        QApplication.processEvents()
    timer.timeout.connect(check_for_interrupt)
    timer.start(100)
    return timer

def table_row_fn(sql_view):
    return lambda row: [QTableWidgetItem(str(row[col])) for col in sql_view.columns]

CAN_CHANGE_CELL = True
def cell_changed_fn(table, row, col):
    global CAN_CHANGE_CELL
    if not CAN_CHANGE_CELL:
        return
    item = table.item(row, col)
    if item:
        onchange = item.data(Qt.ItemDataRole.UserRole)
        # old = TABLE_DATA[table][row]
        if onchange:
            CAN_CHANGE_CELL = False
            onchange(item)
            CAN_CHANGE_CELL = True

def reset_table(table, sql_view, row_fn):
    table.setRowCount(0)
    global TABLE_DATA
    TABLE_DATA[table] = []
    for row in sql_view:
        insert_table_row(table, sql_view, row, row_fn)

# row_fn returns a list of QTableWidgetItems
def Table(sql_view, row_fn=None, header=None):
    if row_fn is None:
        row_fn = table_row_fn(sql_view)
    r = QTableWidget()
    columns = sql_view.columns
    if not header:
        header = columns
    r.setColumnCount(len(header))
    for i, column in enumerate(header):
        r.setHorizontalHeaderItem(i, QTableWidgetItem(column))
    
    cbs = []
    if isinstance(sql_view, rsql.Sort):
        for index, row in enumerate(sql_view):
            insert_sorted_table_row(r, index, row, row_fn)
        cbs.append(sql_view.on_insert(lambda index, row: insert_sorted_table_row(r, index, row, row_fn)))
        cbs.append(sql_view.on_delete(lambda index, _: delete_sorted_table_row(r, index)))
        cbs.append(sql_view.on_update(lambda oind, nind, _, new: update_sorted_table_row(r, oind, nind, new, row_fn)))
        r.cellChanged.connect(lambda row2, col2: cell_changed_fn(r, row2, col2))
    else:
        for row in sql_view:
            insert_table_row(r, sql_view, row, row_fn)
        cbs.append(sql_view.on_update(lambda old, new: update_table_row(r, sql_view, old, new, row_fn)))
        cbs.append(sql_view.on_delete(lambda row: delete_table_row(r, sql_view, row)))
        cbs.append(sql_view.on_reset(lambda: reset_table(r, sql_view, row_fn)))
        cbs.append(sql_view.on_insert(lambda row: insert_table_row(r, sql_view, row, row_fn)))
        r.cellChanged.connect(lambda row2, col2: cell_changed_fn(r, row2, col2))
    r.destroyed.connect(lambda: [cb() for cb in cbs])
    return r

def is_checked(item):
    return item.checkState() == Qt.CheckState.Checked

def latency(fn, msg=None):
    t=time.time()
    fn()
    print(f"latency: {msg} {time.time()-t}")

def table_widget_item(text=None, background=None, foreground=None, font=None,
                      checked=None, checkable=None, onchange=None):
    item = QTableWidgetItem(str(text) if text is not None else None)
    if isinstance(text, rsql.EditableRowItem):
        if onchange:
            onchange = lambda item: (latency(lambda: text.set(item.text()), msg="text.set"), onchange(item))
        else:
            onchange = lambda item: latency(lambda: text.set(item.text()), msg="text.set")
    if background:
        item.setBackground(background)
    if foreground:
        item.setForeground(foreground)
    if font:
        item.setFont(font)
    if checked is not None:
        item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
        checked_value = checked
        if isinstance(checked, rsql.EditableRowItem):
            checked_value = checked.value
            if onchange:
                onchange = lambda item: (latency(lambda: checked.set(is_checked(item)), msg="checked.set"), onchange(item))
            else:
                onchange = lambda item: latency(lambda: checked.set(is_checked(item)), msg="checked.set")
        item.setCheckState(Qt.CheckState.Checked if checked_value else Qt.CheckState.Unchecked)
    if checkable is not None:
        if checkable:
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable) 
        else:
            item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsUserCheckable)
    if onchange:
        item.setData(Qt.ItemDataRole.UserRole, onchange)
    return item

def show_if(condition, widget):
    if isinstance(condition, rsql.Value):
        condition.onvalue(lambda new: widget.setVisible(not not new))
    else:
        widget.setVisible(condition)
    return widget

def show_unless(condition, widget):
    if isinstance(condition, rsql.Value):
        condition.onvalue(lambda new: widget.setVisible(not new))
    else:
        widget.setVisible(not condition)
    return widget

def ButtonGroup(*buttons):
    group = widget(QButtonGroup)
    for button in buttons:
        group.addButton(button)
    return group

def RadioButton(text, onclick=None, checked=False, **kwargs):
    return widget(QRadioButton, text=text, checked=checked, onclick=onclick, **kwargs)

def ComboBox(items, onchange=None, **kwargs):
    cbox = widget(QComboBox, **kwargs)
    cbox.addItems([str(item) for item in items])
    if onchange:
        cbox.currentIndexChanged.connect(onchange)
    return cbox

def add_item(tree_widget, parent_item, row_fn, row):
    if row_fn:
        widget = row_fn(row)
    else:
        widget = str(row)
    item = QTreeWidgetItem(tree_widget if parent_item is None else parent_item, [widget] if isinstance(widget, str) else [])
    if isinstance(widget, QWidget):
        tree_widget.setItemWidget(item, 0, widget)
    item.setData(0, Qt.ItemDataRole.UserRole, row)
    return item

def insert_sorted_tree_row(tree_widget, children_map, row_fn, row, items_by_id):
    parent_id = row.parent_id
    if parent_id in children_map:
        children_map[parent_id].append(row)
    else:
        children_map[parent_id] = [row]
    items_by_id[row.id] = add_item(tree_widget, items_by_id.get(parent_id, tree_widget), row_fn, row)

def delete_tree_row(tree_widget, items_by_id, row, onselect):
    item = items_by_id[row.id]
    parent = item.parent()
    if parent:
        parent.removeChild(item)
    else:
        tree_widget.takeTopLevelItem(tree_widget.indexOfTopLevelItem(item))
    del items_by_id[row.id]
    if onselect:
        onselect(None)

def update_tree_row(tree_widget, children_map, row_fn, old, new, items_by_id, onselect):
    if old.parent_id != new.parent_id:
        delete_tree_row(tree_widget, items_by_id, old, None)
        insert_sorted_tree_row(tree_widget, children_map, row_fn, new, items_by_id)
        if onselect:
            onselect(new)
    else:
        item = items_by_id[new.id]
        original_row = item.data(0, Qt.ItemDataRole.UserRole)
        print_widget_hierarchy(item)
        print("original_row.__updating__", original_row.__updating__)
        if original_row.__updating__:
            return
        if row_fn:
            widget = row_fn(new)
        else:
            widget = str(new)   
        item.setText(0, str(widget) if isinstance(widget, str) else "")
        if isinstance(widget, QWidget):
            tree_widget.setItemWidget(item, 0, widget)
        item.setData(0, Qt.ItemDataRole.UserRole, new)
        if onselect:
            onselect(new)

def TreeWidget(table, row_fn, onselect=None, header_label="", onenter=None):
    tree_widget = QTreeWidget()
    tree_widget.setHeaderLabel(header_label)
    tree_widget.setColumnCount(1)
    children_map = {}
    items_by_id = {}

    def build_tree():
        tree_widget.clear()
        items_by_id.clear()
        children_map.clear()
        # Fetch all rows
        rows = list(table)
        # Build parent-child mapping
        for row in rows:
            parent_id = row.parent_id
            if parent_id in children_map:
                children_map[parent_id].append(row)
            else:
                children_map[parent_id] = [row]
        # Recursive function to add items
        def add_items(parent_id, parent_item=None):
            for row in children_map.get(parent_id, []):
                item = add_item(tree_widget, parent_item, row_fn, row)
                items_by_id[row.id] = item
                add_items(row.id, item)
        add_items(-1)

    build_tree()

    # Set up callbacks
    def on_change(*args):
        build_tree()

    cbs = []
    cbs.append(table.on_insert(lambda row: insert_sorted_tree_row(tree_widget, children_map, row_fn, row, items_by_id)))
    cbs.append(table.on_update(lambda old, new: update_tree_row(tree_widget, children_map, row_fn, old, new, items_by_id, onselect)))
    cbs.append(table.on_delete(lambda row: delete_tree_row(tree_widget, items_by_id, row, onselect)))
    cbs.append(table.on_reset(on_change))
    tree_widget.destroyed.connect(lambda: [cb() for cb in cbs])
    if onselect:
        tree_widget.itemClicked.connect(lambda item: onselect(item.data(0, Qt.ItemDataRole.UserRole)))
    if onenter:
        tree_widget.itemSelectionChanged.connect(lambda: onenter(tree_widget.currentItem().data(0, Qt.ItemDataRole.UserRole)))
    return tree_widget

class SingleWidgetContainer(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)  # Remove margins
        self.child_widget = None

    def setWidget(self, widget, keep_focus=False):
        if self.child_widget:
            print("SingleWidgetContainer switching widget")
            if keep_focus:
                focus = store_focus(self.child_widget)
                print("focus", focus)
            self.layout.removeWidget(self.child_widget)
            self.child_widget.setParent(None)
            self.child_widget.deleteLater()  # Ensure the old widget is properly deleted
            
        self.child_widget = widget
        if widget:
            self.layout.addWidget(widget)
        if keep_focus:
            restore_focus(self.child_widget, focus)
        

    def widget(self):
        return self.child_widget