import streamlit as st
from pandas import DataFrame
from dataclasses import dataclass
from datetime import datetime
from lib.flink import Changelog
from rich.highlighter import ReprHighlighter
from rich.text import Text
from subprocess import Popen, PIPE
# from textual import on
# from textual.containers import Container
# from textual.message import Message
# from textual.widgets import DataTable, Label, TabbedContent, TabPane, Tree
import shutil
import sys
import re
import time


def formattd(td):
    # Formats a timedelta of e.g. 0:06:27 as: 6m27s
    result = ''
    h, r = divmod(td.seconds, 60 * 60)
    m, s = divmod(r, 60)
    if h:
        result += '%dh' % h
    if m:
        result += '%dm' % m
    if s:
        result += '%ds' % s
    if not result:
        # If it's less than a second, use fractional seconds
        s = td.microseconds / 1e6
        result = '%.2fs' % s
    return result


def copy_to_clipboard(data):
    # This is a hacky way to support copying to the clipboard. There are
    # some Python libraries for this but they were all too finicky to be
    # worth adding as a dependency.
    p = Popen(['pbcopy'], stdout=PIPE, stdin=PIPE, stderr=PIPE)
    # Equivalent to `echo $data | pbcopy` on macOS
    p.communicate(input=bytes(data.encode()))


# class LogEventTree(Tree):
#
#     def __init__(self, event, label='', classes=None):
#         super().__init__('', classes=classes)
#         self.add_event(event, label=label)
#
#     def add_event(self, event, label=None):
#         highlighter = ReprHighlighter()
#         def add_node(name, node, data):
#             if isinstance(data, dict):
#                 node.set_label(Text(name))
#                 for key, value in data.items():
#                     new_node = node.add('')
#                     add_node(key, new_node, value)
#             elif isinstance(data, list):
#                 node.set_label(Text(name))
#                 for index, value in enumerate(data):
#                     new_node = node.add('')
#                     add_node(str(index), new_node, value)
#             else:
#                 node.allow_expand = False
#                 if name:
#                     label = Text.assemble(Text.from_markup(name + ' '), highlighter(repr(data)))
#                 else:
#                     label = Text(repr(data))
#                 node.set_label(label)
#         add_node(label or event['message'], self.root, event)


# class CopyLabel(Label):
#
#     label = 'Copy to clipboard'
#
#     def __init__(self):
#         super().__init__(self.label, classes='copy')
#
#     def reset(self):
#         self.update(self.label)
#         self.set_classes('copy')
#
#     def on_click(self, event):
#         copy_to_clipboard(self.parent.error)
#         self.update('Copied')
#         self.set_classes('copied')
#         self.set_timer(3, self.reset)


# class ErrorEvent(Container):
#
#     def __init__(self, message, event, path=None, classes='error-event'):
#         super().__init__(classes=classes)
#         # We are going to display the top-level error message above the event as
#         # a separate DOM element so that we can include entire multi-line error
#         # messages. The Tree class will only display the first line of the label.
#         self.error = message
#         self.event = LogEventTree(event, label='Detail', classes=classes)
#         self.label = Label(self.error, classes=classes)
#         self.log_label = None
#         if path:
#             # If flinklab.errors.log is set, log all errors to the file it points to.
#             # It's probably not ideal to log in the constructor here, but we do it anyways
#             # so that we can try to get a copy of the error as early as possible. When errors
#             # are hard to reproduce, this is far more important than keeping flinklab stable.
#             with open(path, 'a') as f:
#                 f.write('%s:\n%s\n\n' % (datetime.now(), self.error))
#             self.log_label = Label('Logged to %s' % path, classes='error-log-path')
#
#     def compose(self):
#         yield self.label
#         yield self.event
#         if self.log_label:
#             yield self.log_label
#         # User must have pbcopy installed on their system (macOS ships with it)
#         # in order for this to work
#         if shutil.which('pbcopy'):
#             yield CopyLabel()


# Encapsulates the result of one statement, displaying both changelog
# and table output in their own separate tabs. ResultsLog logs these
# as statements are run in order to display statement history.
class Result():

    # sort_icon = re.compile(r' [▲▼]$')

    # def __init__(self, initial):
    #     super().__init__(classes='tabs-container mounting')
    #     # self.initial = initial
    #     # self.current_sort = None
    #     # self.sort_desc = False

    def compose(self):
        self.table_dt = DataFrame()
        # with TabbedContent(initial=self.initial, classes='tabbed-content') as tc:
        #     self.tabs = tc
        #     with TabPane('Table', id='table', classes='results-tab'):
        #         self.table_dt = DataTable(classes='results-table', show_cursor=False)
        #         self.table_dt.visible = False
        #         yield self.table_dt
        #     with TabPane('Changelog', id='changelog', classes='results-tab'):
        #         self.changelog_dt = DataTable(classes='results-table', show_cursor=False)
        #         self.changelog_dt.visible = False
        #         yield self.changelog_dt

    # def on_mount(self):
    #     # This widget seems slow to mount, which causes some visible jitteriness while
    #     # it's initially being rendered. We instantiate it with a class that collapses
    #     # height down to 0 to work around this, and then remove this class once we're
    #     # done mounting.
    #     self.remove_class('mounting')

    # @on(DataTable.HeaderSelected)
    # def on_header_select(self, event):
    #     table = event.control
    #     # Disallow sorting changelogs
    #     if table == self.changelog_dt:
    #         return
    #     # If we've already sorted on a column, strip out the
    #     # sort icon since we're about to change it
    #     if self.current_sort:
    #         _, k = self.current_sort
    #         column = table.columns[k]
    #         label = self.sort_icon.sub('', str(column.label))
    #         column.label = Text(label)
    #
    #     column_key = event.column_key
    #     sort = (table, column_key)
    #     # If we're already sorted on the column that was just clicked,
    #     # swap the sort order
    #     if self.current_sort == sort:
    #         # User clicked on a column that we're already sorted on,
    #         # so just swap the sort order
    #         self.sort_desc = not self.sort_desc
    #     else:
    #         # We're sorting a new column, default to ascending order
    #         self.sort_desc = False
    #
    #     # Do the actual sort
    #     table.sort(column_key, reverse=self.sort_desc)
    #     # Now label the sort column with an ascending/descending icon
    #     column = table.columns[column_key]
    #     icon = '▼' if self.sort_desc else '▲'
    #     # TODO(derekjn): sometimes header columns aren't wide enough
    #     # to fit the sort icon, in which case it isn't visible (sorting
    #     # still works fine though). Try to find a straighforward way to
    #     # deal with this.
    #     column.label = Text('%s %s' % (column.label, icon))
    #     self.current_sort = sort

    def prepare(self, changelog):
        # Prepare the collapsed changelog table
        self.changelog = changelog
        collapsed = changelog.collapse()
        self.collapsed = collapsed

    def consume(self, rows=None, count=None):
        if not rows:
            rows = self.changelog.consume(count, copy=True)
        # Replace contents of collapsed table DataTable with the updated result
        self.collapsed.update(rows)
        self.table_dt.clear()
        self.table_dt.add_rows(self.collapsed)
        # Changelog display is append only
        self.changelog_dt.add_rows(rows)


# ResultsLog handles the consumption and display of result sets.
# The execution worker sends messages to this class's handler depending on
# how it wants the results consumed/displayed. It also deals with displaying
# error messages since they are rendered in the results area. This class
# is basically a log of result sets, and will keep historical results and
# error messages around until it's cleared by the user.
# class ResultsLog(Container):
#
#     @dataclass
#     class Error(Message, bubble=False):
#         error: str
#         event: dict
#         path: str = None
#
#     @dataclass
#     class BeginResult(Message, bubble=True):
#         step: bool = False
#
#     @dataclass
#     class EndResult(Message, bubble=True):
#         def __init__(self, start, end):
#             super().__init__()
#             self.start = datetime.fromtimestamp(start)
#             self.end = datetime.fromtimestamp(end)
#             self.elapsed = formattd(self.end - self.start)
#
#     @dataclass
#     class Prepare(Message, bubble=False):
#         changelog: Changelog
#
#     @dataclass
#     class Consume(Message, bubble=False):
#         count: int | None = None
#         rows: list = None
#
#     @dataclass
#     class StepModeNextOp(Message, bubble=True):
#         op: str
#
#     @dataclass
#     class ToggleChangelog(Message, bubble=False):
#         pass
#
#     def __init__(self, props, id=None):
#         super().__init__(id=id)
#         self.current = None
#         self.initial = 'changelog' if props.get('flinklab.changelog') else 'table'
#
#     @on(Error)
#     def print_error(self, m):
#         t = ErrorEvent(m.error, m.event, path=m.path)
#         if self.current:
#             self.current.remove()
#         self.mount(t, before=0)
#
#     @on(BeginResult)
#     def handle_begin(self, m):
#         # Selected tab (changelog or table) defaults to whatever the user last used
#         result = Result(self.initial)
#         # Note that Textual guarantees that this will be mounted by the time
#         # the next message is received, but it does not guarantee that it will
#         # be mounted immediately after this call. So we don't do anything else
#         # with it yet.
#         self.mount(result, before=0)
#         self.current = result
#
#     @on(EndResult)
#     def handle_end(self, m):
#         # Add a disabled dummy pane to display statement runtime
#         # next to the table and changelog tabs
#         self.current.tabs.add_pane(TabPane(m.elapsed, disabled=True))
#
#     @on(Prepare)
#     def handle_prepare(self, m):
#         self.current.prepare(m.changelog)
#
#     @on(Consume)
#     def handle_consume(self, m):
#         # Message senders can either give us the number of rows to
#         # consume, or the actual rows it has already consumed. The
#         # latter case is used by step mode, which needs to look at
#         # each row before applying it to the displayed result.
#         if m.rows:
#             self.current.consume(rows=m.rows)
#         else:
#             self.current.consume(count=m.count)
#
#     @on(StepModeNextOp)
#     def handle_next_op(self, m):
#         # StepModeNextOp is the message that identifies the op code of the
#         # incoming row during step mode. We handle this message here and then
#         # bubble it up to the parent only so we can ensure this message is
#         # processed in order relative to other messages being sent. If we only
#         # handled this in the parent, certain messages could arrive out of order
#         # because they'll be using two separate event loops.
#         pass
#
#     @on(ToggleChangelog)
#     def action_toggle_changelog(self):
#         # Sent to us by parent app when user inputs hotkey to toggle changelog/table.
#         # We just set the selected tab to the opposite of what's currently selected.
#         if not self.current:
#             return
#         # TabbedContent doesn't seem to fire a TabActivated message when we
#         # change the active tab here. Otherwise we could just set the active
#         # tab and let handle_tab_activated do the rest.
#         self.initial = 'table' if self.initial == 'changelog' else 'changelog'
#         self.current.tabs.active = self.initial
#
#     @on(TabbedContent.TabActivated)
#     def handle_tab_activated(self, event):
#         # Message fired whenever changelog/table tab is changed by clicking on
#         # a tab (hotkey is handled separately). We use this to default new Results
#         # to whichever display mode was last selected.
#         self.initial = event.control.active
#
#     def consume(self, result, next_row_hook=None):
#         # Consume and display the result based on however it's configured
#         if result.is_file():
#             self.dump(result)
#         elif result.is_step():
#             # We always use live mode for stepping, even if we're processing
#             # a finite result. When stepping but not actually in live mode, live()
#             # will break and exit once it has consumed all rows.
#             # We also use an increment of 1 in step mode, regardless of what
#             # the current value is.
#             result.properties['flinklab.live.increment'] = 1
#             self.live(result, next_row_hook=next_row_hook)
#         elif result.is_live():
#             self.live(result)
#         else:
#             self.print(result)
#
#     def dump(self, result):
#         # Consume the entire changelog and dump its contents to a file.
#         # We create fake output rows here to display the result of this
#         # operation in table form.
#         path = result.properties.get('flinklab.output.path')
#         result.consume()
#         count = result.dump()
#         schema = {
#             'columns': [
#                 {'name': 'path'},
#                 {'name': 'count'},
#             ]
#         }
#         row = {
#         row = {
#             'op': None,
#             'row': [path, count]
#         }
#         dummy = Changelog(schema, [row])
#         self.post_message(ResultsLog.Prepare(dummy))
#         self.post_message(ResultsLog.Consume())
#
#     def live(self, result, next_row_hook=None):
#         # Display output continuosly based on the below configuration:
#         props = result.properties
#         # TODO(derekjn) don't use sys.maxsize here, as this will break if
#         # someone wants to run a statement for longer than 292 billion years
#         timeout = int(props.get('flinklab.live.timeout', sys.maxsize))
#         rate = float(props.get('flinklab.live.rate', 1))
#         inc = int(props.get('flinklab.live.increment', 1))
#         step = props.get('flinklab.step')
#
#         self.post_message(ResultsLog.Prepare(result))
#         start = time.time()
#         while time.time() - start < timeout:
#             if self.app.is_worker_cancelled:
#                 break
#             rows = result.consume(inc, copy=True)
#             if not rows and not props.get('flinklab.live'):
#                 # If we're stepping and not actually in live mode, we're done
#                 break
#             if rows and next_row_hook:
#                 if not next_row_hook(result.columns, rows[0]):
#                     break
#             # We send the actual rows to ResultsLog here (rather than just the
#             # number of rows to consume) because if we're in step mode, we need
#             # this thread to block on next_row_hook, which needs a copy of the
#             # row to preview before stepping.
#             # TODO(derekjn): we should probably just keeep some kind of cursor
#             # attached to the changelog so we don't have to send actual rows
#             # here. Then we'd just tell the handler to consume n rows relative
#             # to its current cursor position.
#             self.post_message(ResultsLog.Consume(rows=rows))
#             if not step:
#                 time.sleep(1 / rate)
#
#     def print(self, changelog):
#         if self.app.is_worker_cancelled:
#             return
#         # TODO(derekjn): make this more memory efficient.
#         # We're just YOLO'ing the entire changelog here.
#         self.post_message(ResultsLog.Prepare(changelog))
#         self.post_message(ResultsLog.Consume())
