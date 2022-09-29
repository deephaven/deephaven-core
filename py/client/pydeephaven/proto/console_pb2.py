# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: deephaven/proto/console.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from pydeephaven.proto import ticket_pb2 as deephaven_dot_proto_dot_ticket__pb2
from pydeephaven.proto import application_pb2 as deephaven_dot_proto_dot_application__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1d\x64\x65\x65phaven/proto/console.proto\x12(io.deephaven.proto.backplane.script.grpc\x1a\x1c\x64\x65\x65phaven/proto/ticket.proto\x1a!deephaven/proto/application.proto\"\x18\n\x16GetConsoleTypesRequest\"0\n\x17GetConsoleTypesResponse\x12\x15\n\rconsole_types\x18\x01 \x03(\t\"i\n\x13StartConsoleRequest\x12<\n\tresult_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12\x14\n\x0csession_type\x18\x02 \x01(\t\"T\n\x14StartConsoleResponse\x12<\n\tresult_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\"\x14\n\x12GetHeapInfoRequest\"`\n\x13GetHeapInfoResponse\x12\x16\n\nmax_memory\x18\x01 \x01(\x03\x42\x02\x30\x01\x12\x18\n\x0ctotal_memory\x18\x02 \x01(\x03\x42\x02\x30\x01\x12\x17\n\x0b\x66ree_memory\x18\x03 \x01(\x03\x42\x02\x30\x01\"M\n\x16LogSubscriptionRequest\x12#\n\x17last_seen_log_timestamp\x18\x01 \x01(\x03\x42\x02\x30\x01\x12\x0e\n\x06levels\x18\x02 \x03(\t\"S\n\x13LogSubscriptionData\x12\x12\n\x06micros\x18\x01 \x01(\x03\x42\x02\x30\x01\x12\x11\n\tlog_level\x18\x02 \x01(\t\x12\x0f\n\x07message\x18\x03 \x01(\tJ\x04\x08\x04\x10\x05\"j\n\x15\x45xecuteCommandRequest\x12=\n\nconsole_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12\x0c\n\x04\x63ode\x18\x03 \x01(\tJ\x04\x08\x02\x10\x03\"w\n\x16\x45xecuteCommandResponse\x12\x15\n\rerror_message\x18\x01 \x01(\t\x12\x46\n\x07\x63hanges\x18\x02 \x01(\x0b\x32\x35.io.deephaven.proto.backplane.grpc.FieldsChangeUpdate\"\xb5\x01\n\x1a\x42indTableToVariableRequest\x12=\n\nconsole_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12\x15\n\rvariable_name\x18\x03 \x01(\t\x12;\n\x08table_id\x18\x04 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.TicketJ\x04\x08\x02\x10\x03\"\x1d\n\x1b\x42indTableToVariableResponse\"\x94\x01\n\x14\x43\x61ncelCommandRequest\x12=\n\nconsole_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12=\n\ncommand_id\x18\x02 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\"\x17\n\x15\x43\x61ncelCommandResponse\"\x93\x03\n\x13\x41utoCompleteRequest\x12V\n\ropen_document\x18\x01 \x01(\x0b\x32=.io.deephaven.proto.backplane.script.grpc.OpenDocumentRequestH\x00\x12Z\n\x0f\x63hange_document\x18\x02 \x01(\x0b\x32?.io.deephaven.proto.backplane.script.grpc.ChangeDocumentRequestH\x00\x12\x63\n\x14get_completion_items\x18\x03 \x01(\x0b\x32\x43.io.deephaven.proto.backplane.script.grpc.GetCompletionItemsRequestH\x00\x12X\n\x0e\x63lose_document\x18\x04 \x01(\x0b\x32>.io.deephaven.proto.backplane.script.grpc.CloseDocumentRequestH\x00\x42\t\n\x07request\"\x84\x01\n\x14\x41utoCompleteResponse\x12`\n\x10\x63ompletion_items\x18\x01 \x01(\x0b\x32\x44.io.deephaven.proto.backplane.script.grpc.GetCompletionItemsResponseH\x00\x42\n\n\x08response\"\x15\n\x13\x42rowserNextResponse\"\xa7\x01\n\x13OpenDocumentRequest\x12=\n\nconsole_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12Q\n\rtext_document\x18\x02 \x01(\x0b\x32:.io.deephaven.proto.backplane.script.grpc.TextDocumentItem\"S\n\x10TextDocumentItem\x12\x0b\n\x03uri\x18\x01 \x01(\t\x12\x13\n\x0blanguage_id\x18\x02 \x01(\t\x12\x0f\n\x07version\x18\x03 \x01(\x05\x12\x0c\n\x04text\x18\x04 \x01(\t\"\xb7\x01\n\x14\x43loseDocumentRequest\x12=\n\nconsole_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12`\n\rtext_document\x18\x02 \x01(\x0b\x32I.io.deephaven.proto.backplane.script.grpc.VersionedTextDocumentIdentifier\"\xc0\x03\n\x15\x43hangeDocumentRequest\x12=\n\nconsole_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12`\n\rtext_document\x18\x02 \x01(\x0b\x32I.io.deephaven.proto.backplane.script.grpc.VersionedTextDocumentIdentifier\x12w\n\x0f\x63ontent_changes\x18\x03 \x03(\x0b\x32^.io.deephaven.proto.backplane.script.grpc.ChangeDocumentRequest.TextDocumentContentChangeEvent\x1a\x8c\x01\n\x1eTextDocumentContentChangeEvent\x12\x46\n\x05range\x18\x01 \x01(\x0b\x32\x37.io.deephaven.proto.backplane.script.grpc.DocumentRange\x12\x14\n\x0crange_length\x18\x02 \x01(\x05\x12\x0c\n\x04text\x18\x03 \x01(\t\"\x93\x01\n\rDocumentRange\x12\x41\n\x05start\x18\x01 \x01(\x0b\x32\x32.io.deephaven.proto.backplane.script.grpc.Position\x12?\n\x03\x65nd\x18\x02 \x01(\x0b\x32\x32.io.deephaven.proto.backplane.script.grpc.Position\"?\n\x1fVersionedTextDocumentIdentifier\x12\x0b\n\x03uri\x18\x01 \x01(\t\x12\x0f\n\x07version\x18\x02 \x01(\x05\"+\n\x08Position\x12\x0c\n\x04line\x18\x01 \x01(\x05\x12\x11\n\tcharacter\x18\x02 \x01(\x05\"\xe4\x02\n\x19GetCompletionItemsRequest\x12=\n\nconsole_id\x18\x01 \x01(\x0b\x32).io.deephaven.proto.backplane.grpc.Ticket\x12L\n\x07\x63ontext\x18\x02 \x01(\x0b\x32;.io.deephaven.proto.backplane.script.grpc.CompletionContext\x12`\n\rtext_document\x18\x03 \x01(\x0b\x32I.io.deephaven.proto.backplane.script.grpc.VersionedTextDocumentIdentifier\x12\x44\n\x08position\x18\x04 \x01(\x0b\x32\x32.io.deephaven.proto.backplane.script.grpc.Position\x12\x12\n\nrequest_id\x18\x05 \x01(\x05\"D\n\x11\x43ompletionContext\x12\x14\n\x0ctrigger_kind\x18\x01 \x01(\x05\x12\x19\n\x11trigger_character\x18\x02 \x01(\t\"\x8a\x01\n\x1aGetCompletionItemsResponse\x12G\n\x05items\x18\x01 \x03(\x0b\x32\x38.io.deephaven.proto.backplane.script.grpc.CompletionItem\x12\x12\n\nrequest_id\x18\x02 \x01(\x05\x12\x0f\n\x07success\x18\x03 \x01(\x08\"\x93\x03\n\x0e\x43ompletionItem\x12\r\n\x05start\x18\x01 \x01(\x05\x12\x0e\n\x06length\x18\x02 \x01(\x05\x12\r\n\x05label\x18\x03 \x01(\t\x12\x0c\n\x04kind\x18\x04 \x01(\x05\x12\x0e\n\x06\x64\x65tail\x18\x05 \x01(\t\x12\x15\n\rdocumentation\x18\x06 \x01(\t\x12\x12\n\ndeprecated\x18\x07 \x01(\x08\x12\x11\n\tpreselect\x18\x08 \x01(\x08\x12\x45\n\ttext_edit\x18\t \x01(\x0b\x32\x32.io.deephaven.proto.backplane.script.grpc.TextEdit\x12\x11\n\tsort_text\x18\n \x01(\t\x12\x13\n\x0b\x66ilter_text\x18\x0b \x01(\t\x12\x1a\n\x12insert_text_format\x18\x0c \x01(\x05\x12Q\n\x15\x61\x64\x64itional_text_edits\x18\r \x03(\x0b\x32\x32.io.deephaven.proto.backplane.script.grpc.TextEdit\x12\x19\n\x11\x63ommit_characters\x18\x0e \x03(\t\"`\n\x08TextEdit\x12\x46\n\x05range\x18\x01 \x01(\x0b\x32\x37.io.deephaven.proto.backplane.script.grpc.DocumentRange\x12\x0c\n\x04text\x18\x02 \x01(\t\"\xc5\x30\n\x10\x46igureDescriptor\x12\x12\n\x05title\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x12\n\ntitle_font\x18\x02 \x01(\t\x12\x13\n\x0btitle_color\x18\x03 \x01(\t\x12\x1b\n\x0fupdate_interval\x18\x07 \x01(\x03\x42\x02\x30\x01\x12\x0c\n\x04\x63ols\x18\x08 \x01(\x05\x12\x0c\n\x04rows\x18\t \x01(\x05\x12Z\n\x06\x63harts\x18\n \x03(\x0b\x32J.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.ChartDescriptor\x12\x0e\n\x06\x65rrors\x18\r \x03(\t\x1a\xad\x05\n\x0f\x43hartDescriptor\x12\x0f\n\x07\x63olspan\x18\x01 \x01(\x05\x12\x0f\n\x07rowspan\x18\x02 \x01(\x05\x12[\n\x06series\x18\x03 \x03(\x0b\x32K.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.SeriesDescriptor\x12\x66\n\x0cmulti_series\x18\x04 \x03(\x0b\x32P.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.MultiSeriesDescriptor\x12W\n\x04\x61xes\x18\x05 \x03(\x0b\x32I.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.AxisDescriptor\x12h\n\nchart_type\x18\x06 \x01(\x0e\x32T.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.ChartDescriptor.ChartType\x12\x12\n\x05title\x18\x07 \x01(\tH\x00\x88\x01\x01\x12\x12\n\ntitle_font\x18\x08 \x01(\t\x12\x13\n\x0btitle_color\x18\t \x01(\t\x12\x13\n\x0bshow_legend\x18\n \x01(\x08\x12\x13\n\x0blegend_font\x18\x0b \x01(\t\x12\x14\n\x0clegend_color\x18\x0c \x01(\t\x12\x0c\n\x04is3d\x18\r \x01(\x08\"[\n\tChartType\x12\x06\n\x02XY\x10\x00\x12\x07\n\x03PIE\x10\x01\x12\x08\n\x04OHLC\x10\x02\x12\x0c\n\x08\x43\x41TEGORY\x10\x03\x12\x07\n\x03XYZ\x10\x04\x12\x0f\n\x0b\x43\x41TEGORY_3D\x10\x05\x12\x0b\n\x07TREEMAP\x10\x06\x42\x08\n\x06_title\x1a\xfe\x04\n\x10SeriesDescriptor\x12^\n\nplot_style\x18\x01 \x01(\x0e\x32J.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.SeriesPlotStyle\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x1a\n\rlines_visible\x18\x03 \x01(\x08H\x00\x88\x01\x01\x12\x1b\n\x0eshapes_visible\x18\x04 \x01(\x08H\x01\x88\x01\x01\x12\x18\n\x10gradient_visible\x18\x05 \x01(\x08\x12\x12\n\nline_color\x18\x06 \x01(\t\x12\x1f\n\x12point_label_format\x18\x08 \x01(\tH\x02\x88\x01\x01\x12\x1f\n\x12x_tool_tip_pattern\x18\t \x01(\tH\x03\x88\x01\x01\x12\x1f\n\x12y_tool_tip_pattern\x18\n \x01(\tH\x04\x88\x01\x01\x12\x13\n\x0bshape_label\x18\x0b \x01(\t\x12\x17\n\nshape_size\x18\x0c \x01(\x01H\x05\x88\x01\x01\x12\x13\n\x0bshape_color\x18\r \x01(\t\x12\r\n\x05shape\x18\x0e \x01(\t\x12\x61\n\x0c\x64\x61ta_sources\x18\x0f \x03(\x0b\x32K.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.SourceDescriptorB\x10\n\x0e_lines_visibleB\x11\n\x0f_shapes_visibleB\x15\n\x13_point_label_formatB\x15\n\x13_x_tool_tip_patternB\x15\n\x13_y_tool_tip_patternB\r\n\x0b_shape_sizeJ\x04\x08\x07\x10\x08\x1a\xec\n\n\x15MultiSeriesDescriptor\x12^\n\nplot_style\x18\x01 \x01(\x0e\x32J.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.SeriesPlotStyle\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x63\n\nline_color\x18\x03 \x01(\x0b\x32O.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.StringMapWithDefault\x12\x64\n\x0bpoint_color\x18\x04 \x01(\x0b\x32O.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.StringMapWithDefault\x12\x64\n\rlines_visible\x18\x05 \x01(\x0b\x32M.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BoolMapWithDefault\x12\x65\n\x0epoints_visible\x18\x06 \x01(\x0b\x32M.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BoolMapWithDefault\x12g\n\x10gradient_visible\x18\x07 \x01(\x0b\x32M.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BoolMapWithDefault\x12k\n\x12point_label_format\x18\x08 \x01(\x0b\x32O.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.StringMapWithDefault\x12k\n\x12x_tool_tip_pattern\x18\t \x01(\x0b\x32O.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.StringMapWithDefault\x12k\n\x12y_tool_tip_pattern\x18\n \x01(\x0b\x32O.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.StringMapWithDefault\x12\x64\n\x0bpoint_label\x18\x0b \x01(\x0b\x32O.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.StringMapWithDefault\x12\x63\n\npoint_size\x18\x0c \x01(\x0b\x32O.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.DoubleMapWithDefault\x12\x64\n\x0bpoint_shape\x18\r \x01(\x0b\x32O.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.StringMapWithDefault\x12l\n\x0c\x64\x61ta_sources\x18\x0e \x03(\x0b\x32V.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.MultiSeriesSourceDescriptor\x1a\x64\n\x14StringMapWithDefault\x12\x1b\n\x0e\x64\x65\x66\x61ult_string\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x0c\n\x04keys\x18\x02 \x03(\t\x12\x0e\n\x06values\x18\x03 \x03(\tB\x11\n\x0f_default_string\x1a\x64\n\x14\x44oubleMapWithDefault\x12\x1b\n\x0e\x64\x65\x66\x61ult_double\x18\x01 \x01(\x01H\x00\x88\x01\x01\x12\x0c\n\x04keys\x18\x02 \x03(\t\x12\x0e\n\x06values\x18\x03 \x03(\x01\x42\x11\n\x0f_default_double\x1a^\n\x12\x42oolMapWithDefault\x12\x19\n\x0c\x64\x65\x66\x61ult_bool\x18\x01 \x01(\x08H\x00\x88\x01\x01\x12\x0c\n\x04keys\x18\x02 \x03(\t\x12\x0e\n\x06values\x18\x03 \x03(\x08\x42\x0f\n\r_default_bool\x1a\xa6\x08\n\x0e\x41xisDescriptor\x12\n\n\x02id\x18\x01 \x01(\t\x12m\n\x0b\x66ormat_type\x18\x02 \x01(\x0e\x32X.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.AxisDescriptor.AxisFormatType\x12`\n\x04type\x18\x03 \x01(\x0e\x32R.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.AxisDescriptor.AxisType\x12h\n\x08position\x18\x04 \x01(\x0e\x32V.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.AxisDescriptor.AxisPosition\x12\x0b\n\x03log\x18\x05 \x01(\x08\x12\r\n\x05label\x18\x06 \x01(\t\x12\x12\n\nlabel_font\x18\x07 \x01(\t\x12\x12\n\nticks_font\x18\x08 \x01(\t\x12\x1b\n\x0e\x66ormat_pattern\x18\t \x01(\tH\x00\x88\x01\x01\x12\r\n\x05\x63olor\x18\n \x01(\t\x12\x11\n\tmin_range\x18\x0b \x01(\x01\x12\x11\n\tmax_range\x18\x0c \x01(\x01\x12\x1b\n\x13minor_ticks_visible\x18\r \x01(\x08\x12\x1b\n\x13major_ticks_visible\x18\x0e \x01(\x08\x12\x18\n\x10minor_tick_count\x18\x0f \x01(\x05\x12$\n\x17gap_between_major_ticks\x18\x10 \x01(\x01H\x01\x88\x01\x01\x12\x1c\n\x14major_tick_locations\x18\x11 \x03(\x01\x12\x18\n\x10tick_label_angle\x18\x12 \x01(\x01\x12\x0e\n\x06invert\x18\x13 \x01(\x08\x12\x14\n\x0cis_time_axis\x18\x14 \x01(\x08\x12{\n\x1c\x62usiness_calendar_descriptor\x18\x15 \x01(\x0b\x32U.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BusinessCalendarDescriptor\"*\n\x0e\x41xisFormatType\x12\x0c\n\x08\x43\x41TEGORY\x10\x00\x12\n\n\x06NUMBER\x10\x01\"C\n\x08\x41xisType\x12\x05\n\x01X\x10\x00\x12\x05\n\x01Y\x10\x01\x12\t\n\x05SHAPE\x10\x02\x12\x08\n\x04SIZE\x10\x03\x12\t\n\x05LABEL\x10\x04\x12\t\n\x05\x43OLOR\x10\x05\"B\n\x0c\x41xisPosition\x12\x07\n\x03TOP\x10\x00\x12\n\n\x06\x42OTTOM\x10\x01\x12\x08\n\x04LEFT\x10\x02\x12\t\n\x05RIGHT\x10\x03\x12\x08\n\x04NONE\x10\x04\x42\x11\n\x0f_format_patternB\x1a\n\x18_gap_between_major_ticks\x1a\xf0\x06\n\x1a\x42usinessCalendarDescriptor\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x11\n\ttime_zone\x18\x02 \x01(\t\x12v\n\rbusiness_days\x18\x03 \x03(\x0e\x32_.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BusinessCalendarDescriptor.DayOfWeek\x12~\n\x10\x62usiness_periods\x18\x04 \x03(\x0b\x32\x64.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BusinessCalendarDescriptor.BusinessPeriod\x12o\n\x08holidays\x18\x05 \x03(\x0b\x32].io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BusinessCalendarDescriptor.Holiday\x1a-\n\x0e\x42usinessPeriod\x12\x0c\n\x04open\x18\x01 \x01(\t\x12\r\n\x05\x63lose\x18\x02 \x01(\t\x1a\xf8\x01\n\x07Holiday\x12m\n\x04\x64\x61te\x18\x01 \x01(\x0b\x32_.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BusinessCalendarDescriptor.LocalDate\x12~\n\x10\x62usiness_periods\x18\x02 \x03(\x0b\x32\x64.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BusinessCalendarDescriptor.BusinessPeriod\x1a\x35\n\tLocalDate\x12\x0c\n\x04year\x18\x01 \x01(\x05\x12\r\n\x05month\x18\x02 \x01(\x05\x12\x0b\n\x03\x64\x61y\x18\x03 \x01(\x05\"g\n\tDayOfWeek\x12\n\n\x06SUNDAY\x10\x00\x12\n\n\x06MONDAY\x10\x01\x12\x0b\n\x07TUESDAY\x10\x02\x12\r\n\tWEDNESDAY\x10\x03\x12\x0c\n\x08THURSDAY\x10\x04\x12\n\n\x06\x46RIDAY\x10\x05\x12\x0c\n\x08SATURDAY\x10\x06\x1a\xb6\x01\n\x1bMultiSeriesSourceDescriptor\x12\x0f\n\x07\x61xis_id\x18\x01 \x01(\t\x12S\n\x04type\x18\x02 \x01(\x0e\x32\x45.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.SourceType\x12\x1c\n\x14partitioned_table_id\x18\x03 \x01(\x05\x12\x13\n\x0b\x63olumn_name\x18\x04 \x01(\t\x1a\xb4\x02\n\x10SourceDescriptor\x12\x0f\n\x07\x61xis_id\x18\x01 \x01(\t\x12S\n\x04type\x18\x02 \x01(\x0e\x32\x45.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.SourceType\x12\x10\n\x08table_id\x18\x03 \x01(\x05\x12\x1c\n\x14partitioned_table_id\x18\x04 \x01(\x05\x12\x13\n\x0b\x63olumn_name\x18\x05 \x01(\t\x12\x13\n\x0b\x63olumn_type\x18\x06 \x01(\t\x12`\n\tone_click\x18\x07 \x01(\x0b\x32M.io.deephaven.proto.backplane.script.grpc.FigureDescriptor.OneClickDescriptor\x1a\x63\n\x12OneClickDescriptor\x12\x0f\n\x07\x63olumns\x18\x01 \x03(\t\x12\x14\n\x0c\x63olumn_types\x18\x02 \x03(\t\x12&\n\x1erequire_all_filters_to_display\x18\x03 \x01(\x08\"\xa6\x01\n\x0fSeriesPlotStyle\x12\x07\n\x03\x42\x41R\x10\x00\x12\x0f\n\x0bSTACKED_BAR\x10\x01\x12\x08\n\x04LINE\x10\x02\x12\x08\n\x04\x41REA\x10\x03\x12\x10\n\x0cSTACKED_AREA\x10\x04\x12\x07\n\x03PIE\x10\x05\x12\r\n\tHISTOGRAM\x10\x06\x12\x08\n\x04OHLC\x10\x07\x12\x0b\n\x07SCATTER\x10\x08\x12\x08\n\x04STEP\x10\t\x12\r\n\tERROR_BAR\x10\n\x12\x0b\n\x07TREEMAP\x10\x0b\"\xd2\x01\n\nSourceType\x12\x05\n\x01X\x10\x00\x12\x05\n\x01Y\x10\x01\x12\x05\n\x01Z\x10\x02\x12\t\n\x05X_LOW\x10\x03\x12\n\n\x06X_HIGH\x10\x04\x12\t\n\x05Y_LOW\x10\x05\x12\n\n\x06Y_HIGH\x10\x06\x12\x08\n\x04TIME\x10\x07\x12\x08\n\x04OPEN\x10\x08\x12\x08\n\x04HIGH\x10\t\x12\x07\n\x03LOW\x10\n\x12\t\n\x05\x43LOSE\x10\x0b\x12\t\n\x05SHAPE\x10\x0c\x12\x08\n\x04SIZE\x10\r\x12\t\n\x05LABEL\x10\x0e\x12\t\n\x05\x43OLOR\x10\x0f\x12\n\n\x06PARENT\x10\x10\x12\x0e\n\nHOVER_TEXT\x10\x11\x12\x08\n\x04TEXT\x10\x12\x42\x08\n\x06_titleJ\x04\x08\x0b\x10\x0cJ\x04\x08\x0c\x10\r2\x8e\x0c\n\x0e\x43onsoleService\x12\x98\x01\n\x0fGetConsoleTypes\x12@.io.deephaven.proto.backplane.script.grpc.GetConsoleTypesRequest\x1a\x41.io.deephaven.proto.backplane.script.grpc.GetConsoleTypesResponse\"\x00\x12\x8f\x01\n\x0cStartConsole\x12=.io.deephaven.proto.backplane.script.grpc.StartConsoleRequest\x1a>.io.deephaven.proto.backplane.script.grpc.StartConsoleResponse\"\x00\x12\x8c\x01\n\x0bGetHeapInfo\x12<.io.deephaven.proto.backplane.script.grpc.GetHeapInfoRequest\x1a=.io.deephaven.proto.backplane.script.grpc.GetHeapInfoResponse\"\x00\x12\x96\x01\n\x0fSubscribeToLogs\x12@.io.deephaven.proto.backplane.script.grpc.LogSubscriptionRequest\x1a=.io.deephaven.proto.backplane.script.grpc.LogSubscriptionData\"\x00\x30\x01\x12\x95\x01\n\x0e\x45xecuteCommand\x12?.io.deephaven.proto.backplane.script.grpc.ExecuteCommandRequest\x1a@.io.deephaven.proto.backplane.script.grpc.ExecuteCommandResponse\"\x00\x12\x92\x01\n\rCancelCommand\x12>.io.deephaven.proto.backplane.script.grpc.CancelCommandRequest\x1a?.io.deephaven.proto.backplane.script.grpc.CancelCommandResponse\"\x00\x12\xa4\x01\n\x13\x42indTableToVariable\x12\x44.io.deephaven.proto.backplane.script.grpc.BindTableToVariableRequest\x1a\x45.io.deephaven.proto.backplane.script.grpc.BindTableToVariableResponse\"\x00\x12\x99\x01\n\x12\x41utoCompleteStream\x12=.io.deephaven.proto.backplane.script.grpc.AutoCompleteRequest\x1a>.io.deephaven.proto.backplane.script.grpc.AutoCompleteResponse\"\x00(\x01\x30\x01\x12\x9b\x01\n\x16OpenAutoCompleteStream\x12=.io.deephaven.proto.backplane.script.grpc.AutoCompleteRequest\x1a>.io.deephaven.proto.backplane.script.grpc.AutoCompleteResponse\"\x00\x30\x01\x12\x98\x01\n\x16NextAutoCompleteStream\x12=.io.deephaven.proto.backplane.script.grpc.AutoCompleteRequest\x1a=.io.deephaven.proto.backplane.script.grpc.BrowserNextResponse\"\x00\x42\x43H\x01P\x01Z=github.com/deephaven/deephaven-core/go/internal/proto/consoleb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'deephaven.proto.console_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'H\001P\001Z=github.com/deephaven/deephaven-core/go/internal/proto/console'
  _GETHEAPINFORESPONSE.fields_by_name['max_memory']._options = None
  _GETHEAPINFORESPONSE.fields_by_name['max_memory']._serialized_options = b'0\001'
  _GETHEAPINFORESPONSE.fields_by_name['total_memory']._options = None
  _GETHEAPINFORESPONSE.fields_by_name['total_memory']._serialized_options = b'0\001'
  _GETHEAPINFORESPONSE.fields_by_name['free_memory']._options = None
  _GETHEAPINFORESPONSE.fields_by_name['free_memory']._serialized_options = b'0\001'
  _LOGSUBSCRIPTIONREQUEST.fields_by_name['last_seen_log_timestamp']._options = None
  _LOGSUBSCRIPTIONREQUEST.fields_by_name['last_seen_log_timestamp']._serialized_options = b'0\001'
  _LOGSUBSCRIPTIONDATA.fields_by_name['micros']._options = None
  _LOGSUBSCRIPTIONDATA.fields_by_name['micros']._serialized_options = b'0\001'
  _FIGUREDESCRIPTOR.fields_by_name['update_interval']._options = None
  _FIGUREDESCRIPTOR.fields_by_name['update_interval']._serialized_options = b'0\001'
  _GETCONSOLETYPESREQUEST._serialized_start=140
  _GETCONSOLETYPESREQUEST._serialized_end=164
  _GETCONSOLETYPESRESPONSE._serialized_start=166
  _GETCONSOLETYPESRESPONSE._serialized_end=214
  _STARTCONSOLEREQUEST._serialized_start=216
  _STARTCONSOLEREQUEST._serialized_end=321
  _STARTCONSOLERESPONSE._serialized_start=323
  _STARTCONSOLERESPONSE._serialized_end=407
  _GETHEAPINFOREQUEST._serialized_start=409
  _GETHEAPINFOREQUEST._serialized_end=429
  _GETHEAPINFORESPONSE._serialized_start=431
  _GETHEAPINFORESPONSE._serialized_end=527
  _LOGSUBSCRIPTIONREQUEST._serialized_start=529
  _LOGSUBSCRIPTIONREQUEST._serialized_end=606
  _LOGSUBSCRIPTIONDATA._serialized_start=608
  _LOGSUBSCRIPTIONDATA._serialized_end=691
  _EXECUTECOMMANDREQUEST._serialized_start=693
  _EXECUTECOMMANDREQUEST._serialized_end=799
  _EXECUTECOMMANDRESPONSE._serialized_start=801
  _EXECUTECOMMANDRESPONSE._serialized_end=920
  _BINDTABLETOVARIABLEREQUEST._serialized_start=923
  _BINDTABLETOVARIABLEREQUEST._serialized_end=1104
  _BINDTABLETOVARIABLERESPONSE._serialized_start=1106
  _BINDTABLETOVARIABLERESPONSE._serialized_end=1135
  _CANCELCOMMANDREQUEST._serialized_start=1138
  _CANCELCOMMANDREQUEST._serialized_end=1286
  _CANCELCOMMANDRESPONSE._serialized_start=1288
  _CANCELCOMMANDRESPONSE._serialized_end=1311
  _AUTOCOMPLETEREQUEST._serialized_start=1314
  _AUTOCOMPLETEREQUEST._serialized_end=1717
  _AUTOCOMPLETERESPONSE._serialized_start=1720
  _AUTOCOMPLETERESPONSE._serialized_end=1852
  _BROWSERNEXTRESPONSE._serialized_start=1854
  _BROWSERNEXTRESPONSE._serialized_end=1875
  _OPENDOCUMENTREQUEST._serialized_start=1878
  _OPENDOCUMENTREQUEST._serialized_end=2045
  _TEXTDOCUMENTITEM._serialized_start=2047
  _TEXTDOCUMENTITEM._serialized_end=2130
  _CLOSEDOCUMENTREQUEST._serialized_start=2133
  _CLOSEDOCUMENTREQUEST._serialized_end=2316
  _CHANGEDOCUMENTREQUEST._serialized_start=2319
  _CHANGEDOCUMENTREQUEST._serialized_end=2767
  _CHANGEDOCUMENTREQUEST_TEXTDOCUMENTCONTENTCHANGEEVENT._serialized_start=2627
  _CHANGEDOCUMENTREQUEST_TEXTDOCUMENTCONTENTCHANGEEVENT._serialized_end=2767
  _DOCUMENTRANGE._serialized_start=2770
  _DOCUMENTRANGE._serialized_end=2917
  _VERSIONEDTEXTDOCUMENTIDENTIFIER._serialized_start=2919
  _VERSIONEDTEXTDOCUMENTIDENTIFIER._serialized_end=2982
  _POSITION._serialized_start=2984
  _POSITION._serialized_end=3027
  _GETCOMPLETIONITEMSREQUEST._serialized_start=3030
  _GETCOMPLETIONITEMSREQUEST._serialized_end=3386
  _COMPLETIONCONTEXT._serialized_start=3388
  _COMPLETIONCONTEXT._serialized_end=3456
  _GETCOMPLETIONITEMSRESPONSE._serialized_start=3459
  _GETCOMPLETIONITEMSRESPONSE._serialized_end=3597
  _COMPLETIONITEM._serialized_start=3600
  _COMPLETIONITEM._serialized_end=4003
  _TEXTEDIT._serialized_start=4005
  _TEXTEDIT._serialized_end=4101
  _FIGUREDESCRIPTOR._serialized_start=4104
  _FIGUREDESCRIPTOR._serialized_end=10317
  _FIGUREDESCRIPTOR_CHARTDESCRIPTOR._serialized_start=4351
  _FIGUREDESCRIPTOR_CHARTDESCRIPTOR._serialized_end=5036
  _FIGUREDESCRIPTOR_CHARTDESCRIPTOR_CHARTTYPE._serialized_start=4935
  _FIGUREDESCRIPTOR_CHARTDESCRIPTOR_CHARTTYPE._serialized_end=5026
  _FIGUREDESCRIPTOR_SERIESDESCRIPTOR._serialized_start=5039
  _FIGUREDESCRIPTOR_SERIESDESCRIPTOR._serialized_end=5677
  _FIGUREDESCRIPTOR_MULTISERIESDESCRIPTOR._serialized_start=5680
  _FIGUREDESCRIPTOR_MULTISERIESDESCRIPTOR._serialized_end=7068
  _FIGUREDESCRIPTOR_STRINGMAPWITHDEFAULT._serialized_start=7070
  _FIGUREDESCRIPTOR_STRINGMAPWITHDEFAULT._serialized_end=7170
  _FIGUREDESCRIPTOR_DOUBLEMAPWITHDEFAULT._serialized_start=7172
  _FIGUREDESCRIPTOR_DOUBLEMAPWITHDEFAULT._serialized_end=7272
  _FIGUREDESCRIPTOR_BOOLMAPWITHDEFAULT._serialized_start=7274
  _FIGUREDESCRIPTOR_BOOLMAPWITHDEFAULT._serialized_end=7368
  _FIGUREDESCRIPTOR_AXISDESCRIPTOR._serialized_start=7371
  _FIGUREDESCRIPTOR_AXISDESCRIPTOR._serialized_end=8433
  _FIGUREDESCRIPTOR_AXISDESCRIPTOR_AXISFORMATTYPE._serialized_start=8207
  _FIGUREDESCRIPTOR_AXISDESCRIPTOR_AXISFORMATTYPE._serialized_end=8249
  _FIGUREDESCRIPTOR_AXISDESCRIPTOR_AXISTYPE._serialized_start=8251
  _FIGUREDESCRIPTOR_AXISDESCRIPTOR_AXISTYPE._serialized_end=8318
  _FIGUREDESCRIPTOR_AXISDESCRIPTOR_AXISPOSITION._serialized_start=8320
  _FIGUREDESCRIPTOR_AXISDESCRIPTOR_AXISPOSITION._serialized_end=8386
  _FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR._serialized_start=8436
  _FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR._serialized_end=9316
  _FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR_BUSINESSPERIOD._serialized_start=8860
  _FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR_BUSINESSPERIOD._serialized_end=8905
  _FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR_HOLIDAY._serialized_start=8908
  _FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR_HOLIDAY._serialized_end=9156
  _FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR_LOCALDATE._serialized_start=9158
  _FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR_LOCALDATE._serialized_end=9211
  _FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR_DAYOFWEEK._serialized_start=9213
  _FIGUREDESCRIPTOR_BUSINESSCALENDARDESCRIPTOR_DAYOFWEEK._serialized_end=9316
  _FIGUREDESCRIPTOR_MULTISERIESSOURCEDESCRIPTOR._serialized_start=9319
  _FIGUREDESCRIPTOR_MULTISERIESSOURCEDESCRIPTOR._serialized_end=9501
  _FIGUREDESCRIPTOR_SOURCEDESCRIPTOR._serialized_start=9504
  _FIGUREDESCRIPTOR_SOURCEDESCRIPTOR._serialized_end=9812
  _FIGUREDESCRIPTOR_ONECLICKDESCRIPTOR._serialized_start=9814
  _FIGUREDESCRIPTOR_ONECLICKDESCRIPTOR._serialized_end=9913
  _FIGUREDESCRIPTOR_SERIESPLOTSTYLE._serialized_start=9916
  _FIGUREDESCRIPTOR_SERIESPLOTSTYLE._serialized_end=10082
  _FIGUREDESCRIPTOR_SOURCETYPE._serialized_start=10085
  _FIGUREDESCRIPTOR_SOURCETYPE._serialized_end=10295
  _CONSOLESERVICE._serialized_start=10320
  _CONSOLESERVICE._serialized_end=11870
# @@protoc_insertion_point(module_scope)
