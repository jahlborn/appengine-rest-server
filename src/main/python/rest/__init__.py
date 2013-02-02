#!/usr/bin/env python
#
# Copyright (c) 2012 Boomi, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Rest handler for appengine Models.

To use with an existing application:

    import rest

    # add a handler for REST calls
    application = webapp.WSGIApplication([
      <... existing webservice urls ...>
      ('/rest/.*', rest.Dispatcher)
    ], ...)

    # configure the rest dispatcher to know what prefix to expect on request
    # urls
    rest.Dispatcher.base_url = '/rest'

    # add all models from the current module, and/or...
    rest.Dispatcher.add_models_from_module(__name__)
    # add all models from some other module, and/or...
    rest.Dispatcher.add_models_from_module(my_model_module)
    # add specific models (with given names)
    rest.Dispatcher.add_models({
      'foo' : FooModel,
      'bar' : BarModel})
    # add specific models (with given names) and restrict the supported methods
    rest.Dispatcher.add_models({
      'foo' : (FooModel, rest.READ_ONLY_MODEL_METHODS),
      'bar' : (BarModel, ['GET_METADATA', 'GET', 'POST', 'PUT'],
      'cache' : (CacheModel, ['GET', 'DELETE'] })

    # use custom authentication/authorization
    rest.Dispatcher.authenticator = MyAuthenticator()
    rest.Dispatcher.authorizer = MyAuthorizer()

"""

import types
import logging
import re
import base64
import cgi
import pickle
import os

from google.appengine.api import memcache
from google.appengine.ext import db
from google.appengine.ext import blobstore
from google.appengine.api import namespace_manager
from google.appengine.ext.db import metadata
from xml.dom import minidom
from datetime import datetime

# use faster json if available
try:
    import json
except ImportError:
    from django.utils import simplejson as json

# compatibility w/ python27 & webapp2
try:
    # we only get the _real_ webapp2 in python27
    if os.environ.get('APPENGINE_RUNTIME') == 'python27':
        import webapp2 as webapp
    else:
        raise ImportError("no webapp2 available")
    COMPAT_WEBAPP2 = True
except ImportError:
    from google.appengine.ext import webapp
    COMPAT_WEBAPP2 = False


def get_instance_type_name(value):
    """Returns the name of the type of the given instance."""
    return get_type_name(type(value))


def get_type_name(value_type):
    """Returns the name of the given type."""
    return value_type.__name__


METADATA_PATH = "metadata"
CONTENT_PATH = "content"
BLOBUPLOADRESULT_PATH = "__blob_result"

MAX_FETCH_PAGE_SIZE = 1000

XML_CLEANSE_PATTERN1 = re.compile(r"^(\d)")
XML_CLEANSE_REPL1 = r"_\1"
XML_CLEANSE_PATTERN2 = re.compile(r"[^a-zA-Z0-9_\-]")
XML_CLEANSE_REPL2 = r"_"

EMPTY_VALUE = object()
MULTI_UPDATE_KEY = object()

KEY_PROPERTY_NAME = "key"
KEY_PROPERTY_TYPE_NAME = "KeyProperty"
KEY_QUERY_FIELD = "__key__"

TYPES_EL_NAME = "types"
TYPE_EL_NAME = "type"
LIST_EL_NAME = "list"
TYPE_ATTR_NAME = "type"
NAME_ATTR_NAME = "name"
BASE_ATTR_NAME = "base"
ETAG_ATTR_NAME = "etag"
PROPERTY_ATTR_NAME = "property"
REQUIRED_ATTR_NAME = "required"
DEFAULT_ATTR_NAME = "default"
INDEXED_ATTR_NAME = "indexed"
MULTILINE_ATTR_NAME = "multiline"
VERBOSENAME_ATTR_NAME = "verbose_name"
REFERENCECLASS_ATTR_NAME = "reference_class"
MODELNS_ATTR_NAME = "model_ns"
ITEM_EL_NAME = "item"

DATA_TYPE_SEPARATOR = ":"

DATE_TIME_SEP = "T"
DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT_NO_MS = "%H:%M:%S"
DATE_TIME_FORMAT_NO_MS = DATE_FORMAT + DATE_TIME_SEP + TIME_FORMAT_NO_MS

TRUE_VALUE = "true"
TRUE_NUMERIC_VALUE = "1"

CONTENT_TYPE_HEADER = "Content-Type"
XML_CONTENT_TYPE = "application/xml"
TEXT_CONTENT_TYPE = "text/plain"
JSON_CONTENT_TYPE = "application/json"
METHOD_OVERRIDE_HEADER = "X-HTTP-Method-Override"
RANGE_HEADER = "Range"
BINARY_CONTENT_TYPE = "application/octet-stream"
FORMDATA_CONTENT_TYPE = "multipart/form-data"
ETAG_HEADER = "ETag"

JSON_TEXT_KEY = "#text"
JSON_ATTR_PREFIX = "@"

XML_ENCODING = "utf-8"
XSD_PREFIX = "xs"
XSD_ATTR_XMLNS = "xmlns:" + XSD_PREFIX
XSD_NS = "http://www.w3.org/2001/XMLSchema"
XSD_SCHEMA_NAME = XSD_PREFIX + ":schema"
XSD_ELEMENT_NAME = XSD_PREFIX + ":element"
XSD_ATTRIBUTE_NAME = XSD_PREFIX + ":attribute"
XSD_COMPLEXTYPE_NAME = XSD_PREFIX + ":complexType"
XSD_SIMPLECONTENT_NAME = XSD_PREFIX + ":simpleContent"
XSD_EXTENSION_NAME = XSD_PREFIX + ":extension"
XSD_SEQUENCE_NAME = XSD_PREFIX + ":sequence"
XSD_ANY_NAME = XSD_PREFIX + ":any"
XSD_ANNOTATION_NAME = XSD_PREFIX + ":annotation"
XSD_APPINFO_NAME = XSD_PREFIX + ":appinfo"
XSD_DOCUMENTATION_NAME = XSD_PREFIX + ":documentation"
XSD_FILTER_PREFIX = "bm"
XSD_ATTR_FILTER_XMLNS = "xmlns:" + XSD_FILTER_PREFIX
XSD_FILTER_NS = "http://www.boomi.com/connector/annotation"
XSD_FILTER_NAME = XSD_FILTER_PREFIX + ":filter"
XSD_ATTR_MINOCCURS = "minOccurs"
XSD_ATTR_MAXOCCURS = "maxOccurs"
XSD_ATTR_NAMESPACE = "namespace"
XSD_ATTR_PROCESSCONTENTS = "processContents"
XSD_ATTR_NOFILTER = "ignore"
XSD_ANY_NAMESPACE = "##any"
XSD_LAX_CONTENTS = "lax"
XSD_NO_MIN = "0"
XSD_SINGLE_MAX = "1"
XSD_NO_MAX = "unbounded"
XSD_NORMAL_STR = XSD_PREFIX + ":normalizedString"
BLOBINFO_TYPE_NAME = "BlobInfo"

REST_MD_URI = "uri:com.boomi.rest"
REST_MD_PREFIX = "rest"
REST_MD_XMLNS = "xmlns:" + REST_MD_PREFIX
REST_MD_METADATA_NAME = REST_MD_PREFIX + ":metadata"
REST_MD_CHOICES_NAME = REST_MD_PREFIX + ":choices"
REST_MD_CHOICE_NAME = REST_MD_PREFIX + ":choice"
REST_MD_DEFAULT_NAME = REST_MD_PREFIX + ":default"
REST_MD_ITEM_NAME = REST_MD_PREFIX + ":item"

ALL_MODEL_METHODS = frozenset(["GET", "POST", "PUT", "DELETE", "GET_METADATA"])
READ_ONLY_MODEL_METHODS = frozenset(["GET", "GET_METADATA"])

READ_EXT_NS = "READ"
WRITE_EXT_NS = "WRITE"
HIDDEN_EXT_NAMESPACES = frozenset([])
READ_ONLY_EXT_NAMESPACES = frozenset([READ_EXT_NS])
FULL_EXT_NAMESPACES = frozenset([READ_EXT_NS, WRITE_EXT_NS])

QUERY_OFFSET_PARAM = "offset"
QUERY_CURSOR_PREFIX = "c_"
QUERY_PAGE_SIZE_PARAM = "page_size"
QUERY_ORDERING_PARAM = "ordering"
QUERY_TERM_PATTERN = re.compile(r"^(f.._)(.+)$")
QUERY_PREFIX = "WHERE "
QUERY_JOIN = " AND "
QUERY_ORDERBY = " ORDER BY "
QUERY_ORDER_SUFFIXES = [" ASC", " DESC"]
QUERY_ORDER_PREFIXES = ["", "-"]
QUERY_ORDER_ASC_IDX = 0
QUERY_ORDER_DSC_IDX = 1
QUERY_LIST_TYPE = "fin_"

QUERY_TYPE_PARAM = "type"
QUERY_TYPE_FULL = "full"
# deprecated value, (really means xml or json depending on headers, use
# "structured" instead)
QUERY_TYPE_XML = "xml"
QUERY_TYPE_STRUCTURED = "structured"

QUERY_BLOBINFO_PARAM = "blobinfo"
QUERY_BLOBINFO_TYPE_KEY = "key"
QUERY_BLOBINFO_TYPE_INFO = "info"

QUERY_CALLBACK_PARAM = "callback"

QUERY_INCLUDEPROPS_PARAM = "include_props"

EXTRA_QUERY_PARAMS = frozenset([QUERY_BLOBINFO_PARAM, QUERY_CALLBACK_PARAM,
                                QUERY_INCLUDEPROPS_PARAM])

QUERY_EXPRS = {
    "feq_": "%s = :%d",
    "flt_": "%s < :%d",
    "fgt_": "%s > :%d",
    "fle_": "%s <= :%d",
    "fge_": "%s >= :%d",
    "fne_": "%s != :%d",
    QUERY_LIST_TYPE: "%s IN :%d"}

DATA_TYPE_TO_PROPERTY_TYPE = {
    "basestring": db.StringProperty,
    "str": db.StringProperty,
    "unicode": db.StringProperty,
    "bool": db.BooleanProperty,
    "int": db.IntegerProperty,
    "long": db.IntegerProperty,
    "float": db.FloatProperty,
    "Key": db.ReferenceProperty,
    "datetime": db.DateTimeProperty,
    "date": db.DateProperty,
    "time": db.TimeProperty,
    "Blob": db.BlobProperty,
    "BlobKey": blobstore.BlobReferenceProperty,
    "ByteString": db.ByteStringProperty,
    "Text": db.TextProperty,
    "User": db.UserProperty,
    "Category": db.CategoryProperty,
    "Link": db.LinkProperty,
    "Email": db.EmailProperty,
    "GeoPt": db.GeoPtProperty,
    "IM": db.IMProperty,
    "PhoneNumber": db.PhoneNumberProperty,
    "PostalAddress": db.PostalAddressProperty,
    "Rating": db.RatingProperty,
    "list": db.ListProperty,
    "tuple": db.ListProperty}

PROPERTY_TYPE_TO_XSD_TYPE = {
    get_type_name(db.StringProperty): XSD_PREFIX + ":string",
    get_type_name(db.BooleanProperty): XSD_PREFIX + ":boolean",
    get_type_name(db.IntegerProperty): XSD_PREFIX + ":long",
    get_type_name(db.FloatProperty): XSD_PREFIX + ":double",
    get_type_name(db.ReferenceProperty): XSD_NORMAL_STR,
    get_type_name(db.DateTimeProperty): XSD_PREFIX + ":dateTime",
    get_type_name(db.DateProperty): XSD_PREFIX + ":date",
    get_type_name(db.TimeProperty): XSD_PREFIX + ":time",
    get_type_name(db.BlobProperty): XSD_PREFIX + ":base64Binary",
    get_type_name(db.ByteStringProperty): XSD_PREFIX + ":base64Binary",
    get_type_name(blobstore.BlobReferenceProperty): BLOBINFO_TYPE_NAME,
    get_type_name(db.TextProperty): XSD_PREFIX + ":string",
    get_type_name(db.UserProperty): XSD_NORMAL_STR,
    get_type_name(db.CategoryProperty): XSD_NORMAL_STR,
    get_type_name(db.LinkProperty): XSD_PREFIX + ":anyURI",
    get_type_name(db.EmailProperty): XSD_NORMAL_STR,
    get_type_name(db.GeoPtProperty): XSD_NORMAL_STR,
    get_type_name(db.IMProperty): XSD_NORMAL_STR,
    get_type_name(db.PhoneNumberProperty): XSD_NORMAL_STR,
    get_type_name(db.PostalAddressProperty): XSD_NORMAL_STR,
    get_type_name(db.RatingProperty): XSD_PREFIX + ":integer",
    KEY_PROPERTY_TYPE_NAME: XSD_NORMAL_STR}

PROPERTY_TYPE_TO_JSON_TYPE = {
    get_type_name(db.IntegerProperty): long,
    get_type_name(db.FloatProperty): float,
    get_type_name(db.RatingProperty): long}


class KeyPseudoType(object):
    """Fake PropertyType class for the KeyHandler class."""

    def __init__(self):
        self.verbose_name = None
        self.default = None
        self.required = True
        self.choices = None
        self.indexed = True
        self.name = KEY_PROPERTY_NAME

    def empty(self, value):
        """Returns True if the value is any value which evaluates to False,
        False otherwise."""
        return not value

KEY_PROPERTY_TYPE = KeyPseudoType()


def parse_date_time(dt_str, dt_format, dt_type, allows_microseconds):
    """Returns a datetime/date/time instance parsed from the given string
    using the given format info."""
    micros = None
    if(allows_microseconds):
        dt_parts = dt_str.rsplit(".", 1)
        dt_str = dt_parts.pop(0)
        if(len(dt_parts) > 0):
            micros = int(dt_parts[0].ljust(6, "0")[:6])
    dtime = datetime.strptime(dt_str, dt_format)
    if(micros):
        dtime = dtime.replace(microsecond=micros)
    if(dt_type is datetime.date):
        dtime = dtime.date()
    elif(dt_type is datetime.time):
        dtime = dtime.time()
    return dtime


def convert_to_valid_xml_name(name):
    """Converts a string to a valid xml element name."""
    name = re.sub(XML_CLEANSE_PATTERN1, XML_CLEANSE_REPL1, name)
    return re.sub(XML_CLEANSE_PATTERN2, XML_CLEANSE_REPL2, name)


def append_child(parent_el, name, content=None, meta=None):
    """Returns a new xml element with the given name and optional text content
    appended to the given parent element."""
    doc = parent_el.ownerDocument
    elm = doc.createElement(name)
    parent_el.appendChild(elm)
    if content:
        txt_node = doc.createTextNode(content)
        elm.appendChild(txt_node)
        if meta:
            txt_node.disp_meta_ = meta
    return elm


def mark_list_node(xml_node):
    """Marks an xml element as the parent node of a list of elements.  Useful
    for getting consistent json output for list contents."""
    xml_node.disp_meta_ = LIST_EL_NAME
    return xml_node


def xsd_append_sequence(parent_el):
    """Returns an XML Schema sub-sequence (complex type, then sequence)
    appended to the given parent element."""
    ctype_el = append_child(parent_el, XSD_COMPLEXTYPE_NAME)
    seq_el = append_child(ctype_el, XSD_SEQUENCE_NAME)
    return seq_el


def xsd_append_nofilter(annotation_el):
    """Returns Boomi XML Schema no filter annotation appended to the given
    parent element."""
    child_el = append_child(annotation_el, XSD_APPINFO_NAME)
    filter_el = append_child(child_el, XSD_FILTER_NAME)
    filter_el.attributes[XSD_ATTR_FILTER_XMLNS] = XSD_FILTER_NS
    filter_el.attributes[XSD_ATTR_NOFILTER] = TRUE_VALUE
    return filter_el


def xsd_append_rest_metadata(annotation_el):
    """Returns rest metadata annotation appended to the given parent
    element."""
    element_el = append_child(annotation_el, XSD_APPINFO_NAME)
    element_el = append_child(element_el, REST_MD_METADATA_NAME)
    element_el.attributes[REST_MD_XMLNS] = REST_MD_URI
    return element_el


def xsd_append_choices(parent_el, prop_handler, choices):
    """Appends a list of choice metadata elements."""
    choice_parent = append_child(parent_el, REST_MD_CHOICES_NAME)

    for choice in choices:
        append_child(choice_parent, REST_MD_CHOICE_NAME,
                     prop_handler.value_to_string(choice))

    return choice_parent


def xsd_append_element(parent_el, name, prop_type_name, min_occurs,
                       max_occurs):
    """Returns an XML Schema element with the given attributes appended to the
    given parent element."""
    element_el = append_child(parent_el, XSD_ELEMENT_NAME)
    element_el.attributes[NAME_ATTR_NAME] = name
    type_name = PROPERTY_TYPE_TO_XSD_TYPE.get(prop_type_name, None)
    if type_name:
        element_el.attributes[TYPE_ATTR_NAME] = type_name
    if min_occurs is not None:
        element_el.attributes[XSD_ATTR_MINOCCURS] = min_occurs
    if max_occurs is not None:
        element_el.attributes[XSD_ATTR_MAXOCCURS] = max_occurs
    return element_el


def xsd_append_attribute(parent_el, name, prop_type_name, def_type=None):
    """Returns an XML Schema attribute with the given attributes appended to
       the given parent element."""
    attr_el = append_child(parent_el, XSD_ATTRIBUTE_NAME)
    attr_el.attributes[NAME_ATTR_NAME] = name
    type_name = PROPERTY_TYPE_TO_XSD_TYPE.get(prop_type_name, def_type)
    if type_name:
        attr_el.attributes[TYPE_ATTR_NAME] = type_name
    return attr_el


def get_node_text(node_list, do_strip=False):
    """Returns the complete text from the given node list (optionally
       stripped) or None if the list is empty."""
    if(len(node_list) == 0):
        return None
    text = u""
    for node in node_list:
        if node.nodeType == node.TEXT_NODE:
            text += node.data
    if do_strip:
        text = text.strip()
    return text


def xml_to_json(xml_doc):
    """Returns a serialized json doc string generated from the given xml
    doc."""
    doc_el = xml_doc.documentElement
    json_doc = {doc_el.nodeName: xml_node_to_json(doc_el)}

    return json.dumps(json_doc)


def xml_node_to_json(xml_node):
    """Returns a json node generated from the given xml element."""
    if((len(xml_node.childNodes) == 1) and
       (xml_node.childNodes[0].nodeType == xml_node.TEXT_NODE)):
        txt_node = xml_node.childNodes[0]
        txt_value = json_value(txt_node, txt_node.data)
        if(len(xml_node.attributes) == 0):
            return txt_value
        else:
            json_node = {}
            json_node[JSON_TEXT_KEY] = txt_value
            xml_attrs_to_json(xml_node, json_node)
            return json_node
    else:
        json_node = {}

        force_list = (hasattr(xml_node, "disp_meta_") and
                      xml_node.disp_meta_ == LIST_EL_NAME)
        for child_xml_node in xml_node.childNodes:
            new_child_json_node = xml_node_to_json(child_xml_node)
            cur_child_json_node = json_node.get(child_xml_node.nodeName, None)
            if(cur_child_json_node is None):
                cur_child_json_node = new_child_json_node
                if(force_list):
                    cur_child_json_node = [cur_child_json_node]
            else:
                # if we have more than one of the same type, turn the children
                # into a list
                if(not is_list_type(cur_child_json_node)):
                    cur_child_json_node = [cur_child_json_node]
                cur_child_json_node.append(new_child_json_node)
            json_node[child_xml_node.nodeName] = cur_child_json_node

        # simplify json output lists if desired
        if(force_list and Dispatcher.simple_json_lists and
           (len(json_node) == 1)):
            item_list = json_node.get(ITEM_EL_NAME, None)
            if item_list is not None:
                json_node = item_list

        xml_attrs_to_json(xml_node, json_node)

        return json_node


def xml_attrs_to_json(xml_node, json_node):
    """Adds any attributes of the given xml node to the given json node."""
    xml_node_attrs = xml_node.attributes
    for attr_name in xml_node_attrs.keys():
        attr_node = xml_node_attrs[attr_name]
        json_node[JSON_ATTR_PREFIX + attr_name] = (
            json_value(attr_node, attr_node.nodeValue))


def json_value(xml_node, xml_node_value):
    """Returns the given xml node's value as the appropriate json type."""
    if hasattr(xml_node, "disp_meta_"):
        json_type = PROPERTY_TYPE_TO_JSON_TYPE.get(
            get_instance_type_name(xml_node.disp_meta_))
        if(json_type):
            xml_node_value = json_type(xml_node_value)
    return xml_node_value


def json_to_xml(json_doc):
    """Returns an xml document generated from the given json doc."""
    json_node = json.load(json_doc)

    impl = minidom.getDOMImplementation()
    doc_el_name = json_node.keys()[0]
    xml_doc = impl.createDocument(None, doc_el_name, None)
    json_node_to_xml(xml_doc.documentElement, json_node[doc_el_name])
    return xml_doc


def json_node_to_xml(xml_node, json_node):
    """Appends an xml node generated from the given json node to the given xml
    node."""
    doc = xml_node.ownerDocument
    if(is_json_value_type(json_node)):
        xml_node.appendChild(doc.createTextNode(unicode(json_node)))
    else:
        for json_node_name, json_node_value in json_node.iteritems():
            if(json_node_name[0] == JSON_ATTR_PREFIX):
                xml_node.attributes[json_node_name[1:]] = json_node_value
            elif(json_node_name == JSON_TEXT_KEY):
                xml_node.appendChild(doc.createTextNode(json_node_value))
            else:
                tmp_xml_node = xml_node
                if(not is_list_type(json_node_value)):
                    json_node_value = [json_node_value]
                elif(Dispatcher.simple_json_lists and
                     (len(json_node_value) > 0) and
                     (json_node_name != ITEM_EL_NAME) and
                     is_json_value_type(json_node_value[0])):
                    # convert "simple" list back to default list format
                    tmp_xml_node = append_child(tmp_xml_node, json_node_name)
                    json_node_name = ITEM_EL_NAME

                for json_node_list_value in json_node_value:
                    child_node = append_child(tmp_xml_node, json_node_name)
                    json_node_to_xml(child_node, json_node_list_value)


def is_json_value_type(obj):
    """Returns True if the given obj is a simple json value type, False
    otherwise."""
    return isinstance(obj, (basestring, int, long, float, complex, bool))


def is_list_type(obj):
    """Returns True if the given obj is a 'list' type of some sort, False
    otherwise."""
    return isinstance(obj, (types.ListType, types.TupleType))


def model_hash_to_str(model_hash):
    """Returns the model hash int as an encoded string."""
    return base64.b64encode(str(model_hash)).strip('=')


class PropertyHandler(object):
    """Base handler for Model properties which manages converting properties
    to and from xml.

    This implementation works for most properties which have simple to/from
    string conversions.

    """

    def __init__(self, property_name, property_type,
                 property_content_type=TEXT_CONTENT_TYPE):
        self.property_name = property_name
        self.storage_name = None
        if self.property_name != property_type.name:
            self.storage_name = property_type.name
        self.property_type = property_type
        self.property_content_type = property_content_type

        # most types can be parsed from stripped strings, but don't strip text
        # data
        self.strip_on_read = True
        if(isinstance(property_type, (db.StringProperty, db.TextProperty))):
            self.strip_on_read = False

    def get_query_field(self):
        """Returns the field name which should be used to query this
        property."""
        return self.property_type.name

    def can_query(self):
        """Returns True if this property can be used as a query filter, False
        otherwise."""
        return self.property_type.indexed

    def get_data_type(self):
        """Returns the type of data this property accepts."""
        return self.property_type.data_type

    def empty(self, value):
        """Tests a property value for empty in a manner specific to the
        property type."""
        return self.property_type.empty(value)

    def get_type_string(self):
        """Returns the type string describing this property."""
        return get_instance_type_name(self.property_type)

    def get_value(self, model):
        """Returns the value for this property from the given model
        instance."""
        return getattr(model, self.property_name)

    def get_value_as_string(self, model):
        """Returns the value for this property from the given model instance
        as a string (or EMPTY_VALUE if the value is empty as defined by this
        property type), used by the default write_xml_value() method."""
        value = self.get_value(model)
        if(self.empty(value)):
            return EMPTY_VALUE
        return self.value_to_string(value)

    def value_to_string(self, value):
        """Returns the given property value as a string, used by the default
        get_value_as_string() method."""
        return unicode(value)

    def value_from_xml_string(self, value):
        """Returns the value for this property from the given string value
        (may be None), used by the default read_xml_value() method."""
        if((value is None) or (self.strip_on_read and not value)):
            return None
        if(not isinstance(value, self.get_data_type())):
            value = self.get_data_type()(value)
        return value

    def value_from_raw_string(self, value):
        """Returns the value for this property from the given 'raw' string
        value (may be None), used by the default value_from_request() method.
        Default impl returns value_from_xml_string(value)."""
        if((value is not None) and self.strip_on_read):
            value = value.strip()
        return self.value_from_xml_string(value)

    def value_for_query(self, value):
        """Returns the value for this property from the given string value
        (may be None), for use in a query filter."""
        return self.value_from_xml_string(value)

    def write_xml_value(self, parent_el, prop_xml_name, model,
                        blob_info_format):
        """Returns the property value from the given model instance converted
        to an xml element and appended to the given parent element."""
        value = self.get_value_as_string(model)
        if(value is EMPTY_VALUE):
            return None
        return append_child(parent_el, prop_xml_name, value,
                            self.property_type)

    def read_xml_value(self, props, prop_el):
        """Adds the value for this property to the given property dict
        converted from an xml element."""
        value = self.value_from_xml_string(get_node_text(prop_el.childNodes,
                                                         self.strip_on_read))
        props[self.property_name] = value

    def write_xsd_metadata(self, parent_el, prop_xml_name):
        """Returns the XML Schema element for this property type appended to
        the given parent element."""
        prop_el = xsd_append_element(parent_el, prop_xml_name,
                                     self.get_type_string(), XSD_NO_MIN,
                                     XSD_SINGLE_MAX)
        annot_el = self.write_xsd_metadata_annotation(prop_el)
        if(not self.can_query()):
            xsd_append_nofilter(annot_el)
        return prop_el

    def write_xsd_metadata_annotation(self, prop_el):
        """Writes the additional property metadata annotation element and
        returns it."""
        annotation_el = append_child(prop_el, XSD_ANNOTATION_NAME)
        element_el = xsd_append_rest_metadata(annotation_el)

        prop_type = self.property_type
        element_el.attributes[REQUIRED_ATTR_NAME] = (
            str(prop_type.required).lower())
        element_el.attributes[INDEXED_ATTR_NAME] = (
            str(prop_type.indexed).lower())

        if prop_type.default is not None:
            element_el.attributes[DEFAULT_ATTR_NAME] = (
                self.value_to_string(prop_type.default))

        if prop_type.verbose_name is not None:
            element_el.attributes[VERBOSENAME_ATTR_NAME] = (
                unicode(prop_type.verbose_name))

        if hasattr(prop_type, "multiline"):
            element_el.attributes[MULTILINE_ATTR_NAME] = (
                str(prop_type.multiline).lower())

        if prop_type.choices:
            xsd_append_choices(element_el, self, prop_type.choices)

        return annotation_el

    def value_to_response(self, dispatcher, prop_xml_name, value, path):
        """Writes the output of a single property to the dispatcher's
        response."""
        dispatcher.set_response_content_type(self.property_content_type)
        dispatcher.response.out.write(value)

    def value_from_request(self, dispatcher, model, path):
        """Writes a single property from the dispatcher's response."""
        value = self.value_from_raw_string(
            dispatcher.request.body_file.getvalue())
        setattr(model, self.property_name, value)


class DateTimeHandler(PropertyHandler):
    """PropertyHandler for datetime/data/time property instances."""

    def __init__(self, property_name, property_type):
        super(DateTimeHandler, self).__init__(property_name, property_type)

        self.format_args = []
        if(isinstance(property_type, db.DateProperty)):
            self.dt_format = DATE_FORMAT
            self.dt_type = datetime.date
            self.allows_microseconds = False
        elif(isinstance(property_type, db.TimeProperty)):
            self.dt_format = TIME_FORMAT_NO_MS
            self.dt_type = datetime.time
            self.allows_microseconds = True
        elif(isinstance(property_type, db.DateTimeProperty)):
            self.dt_format = DATE_TIME_FORMAT_NO_MS
            self.dt_type = datetime
            self.allows_microseconds = True
            self.format_args.append(DATE_TIME_SEP)
        else:
            raise ValueError(
                "unexpected property type %s for DateTimeHandler" %
                property_type)

    def value_to_string(self, value):
        """Returns the datetime/date/time value converted to the relevant iso
        string value."""
        value_str = value.isoformat(*self.format_args)
        # undo python's idiotic formatting irregularity
        if(self.allows_microseconds and not value.microsecond):
            value_str += ".000000"
        return unicode(value_str)

    def value_from_xml_string(self, value):
        """Returns the datetime/date/time parsed from the relevant iso string
        value, or None if the string is empty."""
        if(not value):
            return None
        return parse_date_time(value, self.dt_format, self.dt_type,
                               self.allows_microseconds)


class BooleanHandler(PropertyHandler):
    """PropertyHandler for boolean property instances."""

    def __init__(self, property_name, property_type):
        super(BooleanHandler, self).__init__(property_name, property_type)

    def value_to_string(self, value):
        """Returns the boolean value converted to a string value of 'true' or
        'false'."""
        return unicode(value).lower()

    def value_from_xml_string(self, value):
        """Returns the boolean value parsed from the given string value: True
        for the strings 'true' (any case) and '1', False for all other
        non-empty strings, and None otherwise."""
        if(not value):
            return None
        value = value.lower()
        return ((value == TRUE_VALUE) or (value == TRUE_NUMERIC_VALUE))


class TextHandler(PropertyHandler):
    """PropertyHandler for (large) text property instances."""

    def __init__(self, property_name, property_type):
        super(TextHandler, self).__init__(property_name, property_type)

    def can_query(self):
        """Text properties may not be used in query filters."""
        return False


class ByteStringHandler(PropertyHandler):
    """PropertyHandler for ByteString property instances."""

    def __init__(self, property_name, property_type):
        super(ByteStringHandler, self).__init__(property_name, property_type,
                                                BINARY_CONTENT_TYPE)

    def value_to_string(self, value):
        """Returns a ByteString value converted to a Base64 encoded string."""
        return base64.b64encode(str(value))

    def value_from_xml_string(self, value):
        """Returns a ByteString value parsed from a Base64 encoded string, or
        None if the string is empty."""
        if(not value):
            return None
        return base64.b64decode(value)

    def value_from_raw_string(self, value):
        """Returns the given string."""
        return value


class BlobHandler(ByteStringHandler):
    """PropertyHandler for blob property instances."""

    def __init__(self, property_name, property_type):
        super(BlobHandler, self).__init__(property_name, property_type)

    def can_query(self):
        """Blob properties may not be used in query filters."""
        return False


class BaseReferenceHandler(PropertyHandler):
    """Base PropertyHandler for property instances which work with
    keys/references."""

    def __init__(self, property_name, property_type):
        super(BaseReferenceHandler, self).__init__(property_name,
                                                   property_type)

    def get_data_type(self):
        """Returns the db.Key type."""
        return db.Key

    def get_value(self, model):
        """Returns the key of the referenced model instance."""
        value = self.property_type.get_value_for_datastore(model)
        # for dynamic props, this is still sometimes the referenced object
        # and not the key
        if(value and (not isinstance(value, self.get_data_type()))):
            value = value.key()
        return value


class ReferenceHandler(BaseReferenceHandler):
    """PropertyHandler for reference property instances."""

    def __init__(self, property_name, property_type):
        super(ReferenceHandler, self).__init__(property_name, property_type)

    def write_xsd_metadata_annotation(self, prop_el):
        """Writes the annotation metadata for reference properties."""
        annot_el = super(ReferenceHandler, self).write_xsd_metadata_annotation(
            prop_el)
        appinf_el = annot_el.childNodes[0].childNodes[0]

        reference_class_name = XSD_ANY_NAME
        reference_class = self.property_type.reference_class
        if (reference_class is not None) and (reference_class is not db.Model):
            reference_class_name = reference_class.__name__

        appinf_el.attributes[REFERENCECLASS_ATTR_NAME] = reference_class_name

        return annot_el


class BlobReferenceHandler(BaseReferenceHandler):
    """PropertyHandler for blobstore reference property instances."""

    def __init__(self, property_name, property_type):
        super(BlobReferenceHandler, self).__init__(property_name,
                                                   property_type)

    def get_data_type(self):
        """Returns the blobstore.BlobKey type."""
        return blobstore.BlobKey

    def write_xml_value(self, parent_el, prop_xml_name, model,
                        blob_info_format):
        """Returns an xml element containing the blobstore.BlobKey and
        optionally containing the BlobInfo properties as attributes."""
        blob_key = self.get_value(model)
        if(self.empty(blob_key)):
            return None

        blob_el = append_child(parent_el, prop_xml_name,
                               self.value_to_string(blob_key))

        if(blob_info_format == QUERY_BLOBINFO_TYPE_INFO):
            # include all available blobinfo properties
            blob_info = blobstore.BlobInfo.get(blob_key)
            if blob_info:
                for prop_xml_name, prop_handler in (
                    BLOBINFO_PROP_HANDLERS.iteritems()):
                    attr_value = prop_handler.get_value_as_string(blob_info)
                    if(attr_value is not EMPTY_VALUE):
                        blob_el.attributes[prop_xml_name] = attr_value
                        blob_el.attributes[prop_xml_name].disp_meta_ = (
                            prop_handler.property_type)

        return blob_el

    def write_xsd_metadata(self, parent_el, prop_xml_name):
        """Returns the XML Schema element for this property type appended to
        the given parent element.  Adds the BlobInfo complex type if
        necessary."""

        # add simple element for this property of type BlobInfo
        prop_el = super(BlobReferenceHandler, self).write_xsd_metadata(
            parent_el, prop_xml_name)

        # add the BlobInfo type definition if not already added
        schema_el = parent_el.ownerDocument.documentElement
        has_blob_info = False
        for type_el in schema_el.childNodes:
            if(type_el.attributes[NAME_ATTR_NAME].nodeValue ==
               BLOBINFO_TYPE_NAME):
                has_blob_info = True
                break

        # we need to add the BlobInfo type def
        if not has_blob_info:
            blob_type_el = append_child(schema_el, XSD_COMPLEXTYPE_NAME)
            blob_type_el.attributes[NAME_ATTR_NAME] = BLOBINFO_TYPE_NAME
            ext_el = append_child(append_child(blob_type_el,
                                               XSD_SIMPLECONTENT_NAME),
                                  XSD_EXTENSION_NAME)
            ext_el.attributes[BASE_ATTR_NAME] = XSD_NORMAL_STR
            for prop_xml_name, prop_handler in (
                BLOBINFO_PROP_HANDLERS.iteritems()):
                xsd_append_attribute(ext_el, prop_xml_name,
                                     prop_handler.get_type_string())

        return prop_el

    def value_to_response(self, dispatcher, prop_xml_name, value, path):
        """Writes the output a blobkey property (or the blob contents) to the
        dispatcher's response."""

        if((len(path) > 0) and (path.pop(0) == CONTENT_PATH)):
            blob_info = None
            if value:
                blob_info = blobstore.BlobInfo.get(value)
            dispatcher.serve_blob(blob_info)
            return

        # just return blobinfo key
        super(BlobReferenceHandler, self).value_to_response(
            dispatcher, prop_xml_name, value, path)

    def value_from_request(self, dispatcher, model, path):
        """Writes a single property from the dispatcher's response."""

        if(len(path) == 0):
            # just return blobinfo key
            super(BlobReferenceHandler, self).value_from_request(dispatcher,
                                                                 model, path)
            return

        if(path.pop(0) != CONTENT_PATH):
            raise DispatcherException(404)

        dispatcher.upload_blob(path, model, self.property_name)


class KeyHandler(BaseReferenceHandler):
    """PropertyHandler for primary 'key' of a Model instance."""

    def __init__(self):
        super(KeyHandler, self).__init__(KEY_PROPERTY_NAME, KEY_PROPERTY_TYPE)

    def get_query_field(self):
        """Returns the special key query field name '__key__'"""
        return KEY_QUERY_FIELD

    def get_value(self, model):
        """Returns the key of the given model instance if it has been saved,
        EMPTY_VALUE otherwise."""
        if(not model.is_saved()):
            return EMPTY_VALUE
        return model.key()

    def get_type_string(self):
        """Returns the custom 'KeyProperty' type name."""
        return KEY_PROPERTY_TYPE_NAME


class ListHandler(PropertyHandler):
    """PropertyHandler for lists property instances."""

    def __init__(self, property_name, property_type):
        super(ListHandler, self).__init__(property_name, property_type)
        self.sub_handler = get_property_handler(
            ITEM_EL_NAME,
            DATA_TYPE_TO_PROPERTY_TYPE[
                get_type_name(property_type.item_type)]())

    def get_type_string(self):
        """Returns the type string 'ListProperty:' + <sub_type_string>."""
        return (super(ListHandler, self).get_type_string() +
                DATA_TYPE_SEPARATOR + self.sub_handler.get_type_string())

    def can_query(self):
        """Can query is based on the list element type."""
        return self.sub_handler.can_query()

    def value_for_query(self, value):
        """Returns the value for a query filter based on the list element
        type."""
        return self.sub_handler.value_from_xml_string(value)

    def write_xml_value(self, parent_el, prop_xml_name, model,
                        blob_info_format):
        """Returns a list element containing value elements for the property
        from the given model instance appended to the given parent element."""
        values = self.get_value(model)
        if(not values):
            return None
        list_el = mark_list_node(append_child(parent_el, prop_xml_name))
        for value in values:
            append_child(list_el, ITEM_EL_NAME,
                         self.sub_handler.value_to_string(value),
                         self.sub_handler.property_type)
        return list_el

    def read_xml_value(self, props, prop_el):
        """Adds a list containing the property values to the given property
        dict converted from an xml list element."""
        values = []
        sub_props = {}
        for item_node in prop_el.childNodes:
            if((item_node.nodeType == item_node.ELEMENT_NODE) and
               (str(item_node.nodeName) == ITEM_EL_NAME)):
                self.sub_handler.read_xml_value(sub_props, item_node)
                values.append(sub_props.pop(ITEM_EL_NAME))
        props[self.property_name] = values

    def write_xsd_metadata(self, parent_el, prop_xml_name):
        """Returns the XML Schema list element for this property type
        appended to the given parent element."""
        list_el = super(ListHandler, self).write_xsd_metadata(parent_el,
                                                              prop_xml_name)
        seq_el = xsd_append_sequence(list_el)
        xsd_append_element(seq_el, ITEM_EL_NAME,
                           self.sub_handler.get_type_string(), XSD_NO_MIN,
                           XSD_NO_MAX)
        return list_el

    def write_xsd_metadata_annotation(self, prop_el):
        """Writes the list property metadata annotation element and returns
        it."""
        annotation_el = append_child(prop_el, XSD_ANNOTATION_NAME)
        element_el = xsd_append_rest_metadata(annotation_el)

        prop_type = self.property_type
        element_el.attributes[REQUIRED_ATTR_NAME] = (
            str(prop_type.required).lower())
        element_el.attributes[INDEXED_ATTR_NAME] = (
            str(prop_type.indexed).lower())

        if prop_type.verbose_name is not None:
            element_el.attributes[VERBOSENAME_ATTR_NAME] = (
                unicode(prop_type.verbose_name))

        # list properties always have at least an empty list as default
        default_el = mark_list_node(
            append_child(element_el, REST_MD_DEFAULT_NAME))
        if prop_type.default is not None:
            for val in prop_type.default:
                append_child(default_el, ITEM_EL_NAME,
                             self.sub_handler.value_to_string(val))

        return annotation_el

    def value_to_response(self, dispatcher, prop_xml_name, value, path):
        """Writes this list (or a single element of this list) to the
        dispatcher's response."""

        if(len(path) > 0):
            # get individual list element
            item_value = value[int(path.pop(0))]
            self.sub_handler.value_to_response(dispatcher, ITEM_EL_NAME,
                                               item_value, path)
            return

        # return entire list as xml/json
        impl = minidom.getDOMImplementation()
        doc = None

        try:
            doc = impl.createDocument(None, prop_xml_name, None)
            if(value):
                list_el = mark_list_node(doc.documentElement)
                for val in value:
                    append_child(list_el, ITEM_EL_NAME,
                                 self.sub_handler.value_to_string(val),
                                 self.sub_handler.property_type)
            dispatcher.write_output(dispatcher.doc_to_output(doc))
        finally:
            if doc:
                doc.unlink()

    def value_from_request(self, dispatcher, model, path):
        """Writes this list property (or a single element of the list) from
        the dispatcher's response."""

        if(len(path) > 0):
            # set individual list element
            item_value = self.sub_handler.value_from_raw_string(
                dispatcher.request.body_file.getvalue())
            value = getattr(model, self.property_name)
            item_index = int(path.pop(0))
            if(item_index == len(value)):
                value.append(item_value)
            else:
                value[item_index] = item_value
            return

        # set entire list from xml/json
        doc = dispatcher.input_to_xml()

        try:
            props = {}
            self.read_xml_value(props, doc.documentElement)
            value = props[self.property_name]
            setattr(model, self.property_name, value)
        finally:
            doc.unlink()


# Dynamic Property: "values for query"
# Coerce basic Python types from strings.
DYN_FLOAT_RE = re.compile(r"^[+-]?[0-9]+[.][0-9]+$")
DYN_INTEGER_RE = re.compile(r"^[+-]?[0-9]+$")
# Date-time and Time:
# datetime == "2012-01-31T22:00:01"
# date     == "2012-01-31"
# time     == "22:00:01"
DYN_DATETIME_RE = re.compile(r"^\d{4}[-]\d{2}[-]\d{2}T\d{2}[:]\d{2}[:]\d{2}$")
DYN_DATE_RE = re.compile(r"^\d{4}[-]\d{2}[-]\d{2}$")
DYN_TIME_RE = re.compile(r"^\d{2}[:]\d{2}[:]\d{2}$")
# TODO: complex, ...


class DynamicPropertyHandler(object):
    """PropertyHandler for dynamic properties on Expando models."""

    def __init__(self, property_name):
        self.property_name = property_name
        self.storage_name = None

    def get_query_field(self):
        """Returns the field name which should be used to query this
        property."""
        return self.property_name

    def can_query(self):
        """Dynamic properties can be used in query filters.
        However, only fields with the datatype of the query value would match.
        """
        return True

    def write_xml_value(self, parent_el, prop_xml_name, model,
                        blob_info_format):
        """Returns the property value from the given model instance converted
        to an xml element (with a type attribute) of the appropriate type and
        appended to the given parent element."""
        value = getattr(model, self.property_name)
        prop_handler = self.get_handler(None, value)
        prop_el = prop_handler.write_xml_value(parent_el, prop_xml_name, model,
                                               blob_info_format)
        if prop_el:
            prop_el.attributes[TYPE_ATTR_NAME] = prop_handler.get_type_string()
        return prop_el

    def read_xml_value(self, props, prop_el):
        """Adds the value for this property to the given property dict
        converted from an xml element, either as a StringProperty value if no
        type attribute exists or as the type given in a type attribute."""
        if(prop_el.hasAttribute(TYPE_ATTR_NAME)):
            prop_type = str(prop_el.attributes[TYPE_ATTR_NAME].value)
            self.get_handler(prop_type, None).read_xml_value(props, prop_el)
            return

        props[self.property_name] = get_node_text(prop_el.childNodes)

    def value_for_query(self, value):
        """Returns the value for this property from the given string value
        (may be None), for use in a query filter.  Coerce the string value to
        a basic Python type, and let the Property subclass validate it
        further."""
        if value is None:
            query_value = None
        elif (value[0] == "'" or  value[0] == '"') and value[0] == value[-1]:
            # Quoted values do not get coerced, e.g. "-4".
            query_value = unicode(value[1:-1])
        # Python type coercions:
        elif DYN_FLOAT_RE.match(value.strip()):
            query_value = float(value)
        elif DYN_INTEGER_RE.match(value.strip()):
            query_value = int(value)
        elif DYN_DATETIME_RE.match(value.strip()):
            query_value = parse_date_time(value.strip(),
                                          DATE_TIME_FORMAT_NO_MS,
                                          datetime, True)
        elif DYN_TIME_RE.match(value.strip()):
            query_value = parse_date_time(value.strip(), TIME_FORMAT_NO_MS,
                                          datetime.time, True)
        elif DYN_DATE_RE.match(value.strip()):
            query_value = parse_date_time(value.strip(), DATE_FORMAT,
                                          datetime.date, True)
        else:
            # keep as a string.
            query_value = unicode(value)
        return query_value

    def get_handler(self, property_type, value):
        """Returns the relevant PropertyHandler based on the given
        property_type string or property value."""
        prop_args = []
        sub_handler = None
        if(value is not None):
            property_type = DATA_TYPE_TO_PROPERTY_TYPE[
                get_instance_type_name(value)]
            if(property_type is db.ListProperty):
                prop_args.append(type(value[0]))

        if(isinstance(property_type, basestring)):
            if DATA_TYPE_SEPARATOR in property_type:
                property_type, sub_property_type = property_type.split(
                    DATA_TYPE_SEPARATOR, 1)
                sub_handler = get_property_handler(ITEM_EL_NAME, getattr(
                    db, sub_property_type)())
                prop_args.append(sub_handler.get_data_type())
            property_type = getattr(db, property_type)

        property_type = property_type(*prop_args)
        property_type.name = self.property_name

        return get_property_handler(self.property_name, property_type)


def get_property_handler(property_name, property_type):
    """Returns a PropertyHandler instance with the given name appropriate for
    the given type."""
    if(isinstance(property_type, (db.DateTimeProperty, db.TimeProperty,
                                  db.DateProperty))):
        return DateTimeHandler(property_name, property_type)
    elif(isinstance(property_type, db.BooleanProperty)):
        return BooleanHandler(property_name, property_type)
    elif(isinstance(property_type, db.ReferenceProperty)):
        return ReferenceHandler(property_name, property_type)
    elif(isinstance(property_type, db.ByteStringProperty)):
        return ByteStringHandler(property_name, property_type)
    elif(isinstance(property_type, db.BlobProperty)):
        return BlobHandler(property_name, property_type)
    elif(isinstance(property_type, db.TextProperty)):
        return TextHandler(property_name, property_type)
    elif(isinstance(property_type, db.ListProperty)):
        return ListHandler(property_name, property_type)
    elif(isinstance(property_type, blobstore.BlobReferenceProperty)):
        return BlobReferenceHandler(property_name, property_type)

    return PropertyHandler(property_name, property_type)


class ModelQuery(object):
    """Utility class for holding parameters for a model query."""

    def __init__(self):
        self.fetch_page_size = MAX_FETCH_PAGE_SIZE
        self.fetch_offset = None
        self.fetch_cursor = None
        self.ordering = None
        self.order_type_idx = QUERY_ORDER_ASC_IDX
        self.query_expr = None
        self.query_params = []
        self.next_fetch_offset = ""

    def parse(self, dispatcher, model_handler):
        """Parses the current request into a query."""

        for arg in dispatcher.request.arguments():
            if(arg == QUERY_OFFSET_PARAM):
                query_offset = str(dispatcher.request.get(QUERY_OFFSET_PARAM))
                if query_offset[0:2] == QUERY_CURSOR_PREFIX:
                    self.fetch_cursor = query_offset[2:]
                else:
                    self.fetch_offset = int(query_offset)
                continue

            if(arg == QUERY_PAGE_SIZE_PARAM):
                self.fetch_page_size = int(
                    dispatcher.request.get(QUERY_PAGE_SIZE_PARAM))
                continue

            if(arg == QUERY_ORDERING_PARAM):
                ordering_field = dispatcher.request.get(QUERY_ORDERING_PARAM)
                if(ordering_field[0] == "-"):
                    ordering_field = ordering_field[1:]
                    self.order_type_idx = QUERY_ORDER_DSC_IDX
                prop_handler = model_handler.get_property_handler(
                    ordering_field)
                if(not prop_handler.can_query()):
                    raise KeyError("Can not order on property %s" %
                                   ordering_field)
                self.ordering = prop_handler.get_query_field()
                continue

            if(arg in EXTRA_QUERY_PARAMS):
                #ignore
                continue

            match = QUERY_TERM_PATTERN.match(arg)
            if(match is None):
                logging.warning("ignoring unexpected query param %s", arg)
                continue

            query_type = match.group(1)
            query_field = match.group(2)
            query_sub_expr = QUERY_EXPRS[query_type]

            query_values = dispatcher.request.get_all(arg)
            if(query_type == QUERY_LIST_TYPE):
                query_values = [v.split(",") for v in query_values]

            query_field, query_values = model_handler.read_query_values(
                query_field, query_values)

            for value in query_values:
                self.query_params.append(value)
                query_sub_expr = query_sub_expr % (query_field,
                                                   len(self.query_params))
                if(not self.query_expr):
                    self.query_expr = QUERY_PREFIX + query_sub_expr
                else:
                    self.query_expr += QUERY_JOIN + query_sub_expr

        self.fetch_page_size = max(min(dispatcher.fetch_page_size,
                                       self.fetch_page_size,
                                       MAX_FETCH_PAGE_SIZE), 1)


class Lazy(object):
    """Utility class for enabling lazy initialization of decorated
    properties."""

    def __init__(self, calculate_function):
        self._calculate = calculate_function

    def __get__(self, obj, _=None):
        if obj is None:
            return self
        value = self._calculate(obj)
        setattr(obj, self._calculate.func_name, value)
        return value


class ModelHandler(object):
    """Handler for a Model (or Expando) type which manages converting
    instances to and from xml."""

    def __init__(self, model_name, model_type, model_methods):
        self.model_name = model_name
        self.model_type = model_type
        self.key_handler = KeyHandler()
        self.model_methods = model_methods

    @Lazy
    def property_handlers(self):
        """Lazy initializer for the property_handlers dict."""
        prop_handlers = {}
        for prop_name, prop_type in self.model_type.properties().iteritems():
            prop_handler = get_property_handler(prop_name, prop_type)
            prop_xml_name = convert_to_valid_xml_name(
                prop_handler.property_name)
            prop_handlers[prop_xml_name] = prop_handler
        return prop_handlers

    def is_dynamic(self):
        """Returns True if this Model type supports dynamic properties (is a
        subclass of Expando), False otherwise."""
        return issubclass(self.model_type, db.Expando)

    def get(self, key):
        """Returns the model instance with the given key."""
        model = self.model_type.get(key)
        if model and Dispatcher.enable_etags:
            # compute pristine hash before any modifications are made
            self.hash_model(model)
        return model

    @classmethod
    def put(cls, model):
        """Saves a new/updated model instance."""
        model.put()
        if Dispatcher.enable_etags:
            # compute the new etag for the modified instance
            cls.hash_model(model, True)

    def create(self, props):
        """Returns a newly created model instance with the given properties
        (as a keyword dict)."""

        # convert property names to storage names, since the Model
        # constructor seems to expect properties using the storage name
        # (blech!)
        for prop_handler in self.property_handlers.itervalues():
            if(prop_handler.storage_name and
               (prop_handler.property_name in props)):
                props[prop_handler.storage_name] = props.pop(
                    prop_handler.property_name)

        return self.model_type(**props)

    def get_all(self, model_query):
        """Returns all model instances of this type matching the given
        query."""

        if model_query.fetch_offset is not None:
            # if possible, attempt to fetch more than we really want so that
            # we can determine if we have more results.  this trick is only
            # possible if fetching w/ offsets
            real_fetch_page_size = model_query.fetch_page_size
            if(model_query.fetch_page_size < MAX_FETCH_PAGE_SIZE):
                model_query.fetch_page_size += 1

        if(model_query.query_expr is None):
            query = self.model_type.all()
            if(model_query.ordering):
                query.order(QUERY_ORDER_PREFIXES[model_query.order_type_idx] +
                            model_query.ordering)
        else:
            if(model_query.ordering):
                model_query.query_expr += (
                    QUERY_ORDERBY + model_query.ordering +
                    QUERY_ORDER_SUFFIXES[model_query.order_type_idx])
            query = self.model_type.gql(model_query.query_expr,
                                        *model_query.query_params)

        if model_query.fetch_offset is None:
            if model_query.fetch_cursor:
                query.with_cursor(model_query.fetch_cursor)

            models = query.fetch(model_query.fetch_page_size)

            if(len(models) == model_query.fetch_page_size):
                try:
                    model_query.next_fetch_offset = (QUERY_CURSOR_PREFIX +
                                                     query.cursor())
                except AssertionError:
                    # some queries don't allow cursors, fallback to offsets
                    model_query.next_fetch_offset = str(
                        model_query.fetch_page_size)

        else:
            models = query.fetch(model_query.fetch_page_size,
                                 model_query.fetch_offset)

            if(len(models) >= model_query.fetch_page_size):
                model_query.next_fetch_offset = str(real_fetch_page_size +
                                                    model_query.fetch_offset)

            # trim list to the actual size we want
            if(len(models) > real_fetch_page_size):
                models = models[0:real_fetch_page_size]

        return models

    def delete_all(self, model_query):
        """Deletes all model instances of this type matching the given
        query."""
        if(model_query.query_expr is None):
            query = self.model_type.all()
        else:
            query = self.model_type.gql(model_query.query_expr,
                                        *model_query.query_params)

        db.delete(query)

    def get_property_handler(self, prop_name):
        """Returns the relevant property handler for the given property
        name."""
        prop_name = str(prop_name)
        if(prop_name == KEY_PROPERTY_NAME):
            return self.key_handler
        elif(prop_name in self.property_handlers):
            return self.property_handlers[prop_name]
        elif(self.is_dynamic()):
            return DynamicPropertyHandler(prop_name)
        else:
            raise KeyError("Unknown property %s" % prop_name)

    def read_xml_value(self, model_el):
        """Returns a property dictionary for this Model from the given model
        element."""
        props = {}
        for prop_node in model_el.childNodes:
            if(prop_node.nodeType != prop_node.ELEMENT_NODE):
                continue
            prop_xml_name = prop_node.nodeName
            if prop_xml_name[0] == "_":
                # ignore incoming properties which start with underscore,
                # since this is an invalid property name anyway
                continue
            self.get_property_handler(prop_xml_name).read_xml_value(
                props, prop_node)

        return props

    def read_xml_property(self, prop_el, props, prop_handler):
        """Reads a property from a property element."""
        prop_handler.read_xml_value(props, prop_el)

    def read_query_values(self, prop_query_name, prop_query_values):
        """Returns a tuple of (query_field, query_values) from the given
        query property name and value list."""
        prop_handler = self.get_property_handler(prop_query_name)

        if(not prop_handler.can_query()):
            raise KeyError("Can not filter on property %s" % prop_query_name)

        return (prop_handler.get_query_field(),
                [self.read_query_value(prop_handler, v)
                 for v in prop_query_values])

    def read_query_value(self, prop_handler, prop_query_value):
        """Returns a query value from the given query property handler and
        value (may be a list)."""
        if is_list_type(prop_query_value):
            return [prop_handler.value_for_query(v) for v in prop_query_value]
        else:
            return prop_handler.value_for_query(prop_query_value)

    def write_xml_value(self, model_el, model, blob_info_format,
                        include_props):
        """Appends the properties of the given instance as xml elements to
        the given model element."""

        # if namespaces are readable externally, set relevant attr
        if READ_EXT_NS in Dispatcher.external_namespaces:
            model_ns = None
            if model.is_saved():
                model_ns = model.key().namespace()
            if model_ns:
                model_el.attributes[MODELNS_ATTR_NAME] = model_ns

        # add etag attr if enabled
        if Dispatcher.enable_etags:
            model_el.attributes[ETAG_ATTR_NAME] = model_hash_to_str(
                self.hash_model(model))

        # write key property first
        if((include_props is None) or (KEY_PROPERTY_NAME in include_props)):
            self.write_xml_property(model_el, model, KEY_PROPERTY_NAME,
                                    self.key_handler, blob_info_format)

        # write static properties next
        for prop_xml_name, prop_handler in self.property_handlers.iteritems():
            if((include_props is None) or (prop_xml_name in include_props)):
                self.write_xml_property(model_el, model, prop_xml_name,
                                        prop_handler, blob_info_format)

        # write dynamic properties last
        for prop_name in model.dynamic_properties():
            prop_xml_name = convert_to_valid_xml_name(prop_name)
            if((include_props is None) or (prop_xml_name in include_props)):
                self.write_xml_property(model_el, model, prop_xml_name,
                                        DynamicPropertyHandler(prop_name),
                                        blob_info_format)

    def write_xml_property(self, model_el, model, prop_xml_name, prop_handler,
                           blob_info_format):
        """Writes a property as a property element."""
        prop_handler.write_xml_value(model_el, prop_xml_name, model,
                                     blob_info_format)

    def write_xsd_metadata(self, type_el, model_xml_name):
        """Appends the XML Schema elements of the property types of this
        model type to the given parent element."""
        top_el = append_child(type_el, XSD_ELEMENT_NAME)
        top_el.attributes[NAME_ATTR_NAME] = model_xml_name

        if Dispatcher.include_docstring_in_schema and self.model_type.__doc__:
            annotation_el = append_child(top_el, XSD_ANNOTATION_NAME)
            append_child(annotation_el, XSD_DOCUMENTATION_NAME,
                         self.model_type.__doc__)

        seq_el = xsd_append_sequence(top_el)

        self.key_handler.write_xsd_metadata(seq_el, KEY_PROPERTY_NAME)

        for prop_xml_name, prop_handler in self.property_handlers.iteritems():
            prop_handler.write_xsd_metadata(seq_el, prop_xml_name)

        if(self.is_dynamic()):
            any_el = append_child(seq_el, XSD_ANY_NAME)
            any_el.attributes[XSD_ATTR_NAMESPACE] = XSD_ANY_NAMESPACE
            any_el.attributes[XSD_ATTR_PROCESSCONTENTS] = XSD_LAX_CONTENTS
            any_el.attributes[XSD_ATTR_MINOCCURS] = XSD_NO_MIN
            any_el.attributes[XSD_ATTR_MAXOCCURS] = XSD_NO_MAX

        if READ_EXT_NS in Dispatcher.external_namespaces:
            xsd_append_attribute(seq_el.parentNode, MODELNS_ATTR_NAME, None,
                                 XSD_NORMAL_STR)

        if Dispatcher.enable_etags:
            xsd_append_attribute(seq_el.parentNode, ETAG_ATTR_NAME, None,
                                 XSD_NORMAL_STR)

    @classmethod
    def hash_model(cls, model, force_rehash=False):
        """Returns a hash of the model, suitable for an etag value."""
        # after computing the hash, we store it on the model for future
        # retrieval (useful if the model is later modified)
        if((not hasattr(model, "model_hash_")) or force_rehash):
            model.model_hash_ = cls.hash_model_impl(model)
        return model.model_hash_

    @classmethod
    def hash_model_impl(cls, model):
        """Returns a hash of the model, suitable for an etag value."""
        # if entity versions are available, use them
        entity_version = metadata.get_entity_group_version(model)
        if entity_version:
            return entity_version

        # otherwise, create hash of all model props (and key)
        model_hash = 0
        for prop_key, prop_value in db.to_dict(model).iteritems():
            prop_hash = hash(prop_key)
            if is_list_type(prop_value):
                for idx in range(0, len(prop_value)):
                    prop_hash += (idx * hash(prop_value[idx]))
            else:
                prop_hash += hash(prop_value)
            model_hash = model_hash ^ prop_hash

        model_hash = model_hash ^ (hash(KEY_PROPERTY_NAME) + hash(model.key()))

        return model_hash


# static collection of property handlers for BlobInfo types (because
# BlobInfo.properties() is a set not a dict)
BLOBINFO_PROP_HANDLERS = {
    "content_type": PropertyHandler("content_type", db.StringProperty()),
    "creation": DateTimeHandler("creation", db.DateTimeProperty()),
    "filename": PropertyHandler("filename", db.StringProperty()),
    "size": PropertyHandler("size", db.IntegerProperty())}


class DispatcherException(Exception):
    """Exception which contains an http error code to be returned from the
    current request.  If error_code is None, the thrower is assumed to have
    configured the response appropriately before throwing."""

    def __init__(self, error_code=None):
        super(DispatcherException, self).__init__()
        self.error_code = error_code


class Authenticator(object):
    """Handles authentication of REST API calls."""

    def authenticate(self, dispatcher):
        """Authenticates the current request for the given dispatcher.
        Returns if authentication succeeds, otherwise raises a
        DispatcherException with an appropriate error code, e.g. 403 or 404
        (see the Dispatcher.forbidden() and Dispatcher.not_found() methods).
        Note, an error_code of None is handled specially by the Dispatcher
        (the response is not modified) so that, for example, the
        Authenticator could issue an HTTP authentication challenge by
        configuring the response appropriately and then throwing a
        DispatcherException with no code.

        Args:
          dispatcher: the dispatcher for the request to be authenticated
        """
        pass


class Authorizer(object):
    """Handles authorization for REST API calls.  In general, authorization
    failures in can_* methods should raise a DispatcherException with an
    appropriate error code while filter_* methods should simply remove any
    unauthorized data."""

    def can_read_metadata(self, dispatcher, model_name):
        """Returns if the metadata of the model with the given model_name is
        visible to the user associated with the current request for the given
        dispatcher, otherwise raises a DispatcherException with an
        appropriate error code (see the Dispatcher.forbidden() method).

        Args:
          dispatcher: the dispatcher for the request to be authorized
          model_name: the name of the model whose metadata has been requested
        """
        pass

    def filter_read_metadata(self, dispatcher, model_names):
        """Returns the model_names from the given list whose metadata is
        visible to the user associated with the current request for the given
        dispatcher.

        Args:
          dispatcher: the dispatcher for the request to be authorized
          model_names: the names of models whose metadata has been requested
        """
        return model_names

    def can_read(self, dispatcher, model):
        """Returns if the given model can be read by the user associated with
        the current request for the given dispatcher, otherwise raises a
        DispatcherException with an appropriate error code (see the
        Dispatcher.forbidden() method).

        Args:
          dispatcher: the dispatcher for the request to be authorized
          model: the model to be read
        """
        pass

    def filter_read(self, dispatcher, models):
        """Returns the models from the given list which can be read by the
        user associated with the current request for the given dispatcher.
        Note, the check_query() method can also be used to filter the models
        retrieved from a query.

        Args:
          dispatcher: the dispatcher for the request to be authorized
          models: the models to be read
        """
        return models

    def check_query(self, dispatcher, query_expr, query_params):
        """Verifies/modifies the given query so that it is valid for the user
        associated with the current request for the given dispatcher.  For
        instance, if every model has an 'owner' field, an implementation of
        this method could be:

            query_params.append(authenticated_user)
            if(not query_expr):
                query_expr = 'WHERE owner = :%d' % (len(query_params))
            else:
                query_expr += ' AND owner = :%d' % (len(query_params))
            return query_expr

        Args:
          dispatcher: the dispatcher for the request to be authorized
          query_expr: currently defined query expression, like 'WHERE foo = :1
                      AND blah = :2', or None for 'query all'
          query_params: the list of positional query parameters associated with
                        the given query_expr
        """
        return query_expr

    def can_write(self, dispatcher, model, is_replace):
        """Returns if the given model can be modified by the user associated
        with the current request for the given dispatcher, otherwise raises a
        DispatcherException with an appropriate error code (see the
        Dispatcher.forbidden() method).

        Args:
          dispatcher: the dispatcher for the request to be authorized
          model: the model to be modified
          is_replace: True if this is a full update (PUT), False otherwise
                      (POST)
        """
        pass

    def filter_write(self, dispatcher, models, is_replace):
        """Returns the models from the given list which can be modified by
        the user associated with the current request for the given
        dispatcher.

        Args:
          dispatcher: the dispatcher for the request to be authorized
          models: the models to be modified
          is_replace: True if this is a full update (PUT), False otherwise
                      (POST)
        """
        return models

    def can_write_blobinfo(self, dispatcher, model, property_name):
        """Returns if the a blob for the given property_name on the given
        model can be uploaded by the user associated with the current request
        for the given dispatcher, otherwise raises a DispatcherException with
        an appropriate error code (see the Dispatcher.forbidden() method).
        This call is a pre-check _before_ the blob is uploaded (there will be
        another, normal can_write() check after the upload succeeds).

        Args:
          dispatcher: the dispatcher for the request to be authorized
          model: the model to be (eventually) modified
          property_name: the name of the BlobInfo to be uploaded.
        """
        pass

    def can_delete(self, dispatcher, model_type, model_key):
        """Returns if the given model can be deleted by the user associated
        with the current request for the given dispatcher, otherwise raises a
        DispatcherException with an appropriate error code (see the
        Dispatcher.forbidden() method).

        Args:
          dispatcher: the dispatcher for the request to be authorized
          model_type: the class of the model to be be deleted
          model_key: the key of the model to be deleted
        """
        pass

    def check_delete_query(self, dispatcher, query_expr, query_params):
        """Verifies/modifies the given delete query so that it is valid for
        the user associated with the current request for the given
        dispatcher.  See check_query() for example usage.

        Args:
          dispatcher: the dispatcher for the request to be authorized
          query_expr: currently defined delete query expression, like
                      'WHERE foo = :1 AND blah = :2', or None for 'query all'
          query_params: the list of positional query parameters associated with
                        the given query_expr
        """
        return query_expr


class CachedResponse(object):
    """Simple class used to cache query responses."""

    def __init__(self, dispatcher):
        request = dispatcher.request
        response = dispatcher.response
        if not COMPAT_WEBAPP2:
            self.out = response.out.getvalue()
        else:
            self.out = response.out.body
        self.content_type = response.disp_out_type_
        self.accept = unicode(request.accept)
        if Dispatcher.enable_etags:
            self.etag = response.headers[ETAG_HEADER]

    def matches_request(self, request):
        """Checks if the given request acceptably matches the request which
        generated this cached response."""
        # for now we just require the "accept" header to match in order to use
        # a cached response
        return self.accept == unicode(request.accept)

    def is_not_modified(self, dispatcher):
        """Checks if the cache response is unmodified with respect to the
        given request."""
        return (Dispatcher.enable_etags and
                (self.etag.strip('"') in dispatcher.request.if_none_match))

    def write_output(self, dispatcher):
        """Writes this cached response to the current response output of the
        dispatcher."""
        dispatcher.response.out.write(self.out)
        dispatcher.response.headers[CONTENT_TYPE_HEADER] = self.content_type
        if self.etag:
            dispatcher.response.headers[ETAG_HEADER] = self.etag


class Dispatcher(webapp.RequestHandler):
    """RequestHandler which presents a REST based API for interacting with
    the datastore of a Google App Engine application.

    Integrating this handler with an existing application is designed to be
    as simple as possible.  The user merely needs to configure this class
    with the relevant model instances using one of the add_model*() class
    methods.  Example usage is include in the module level documentation.

    This handler has builtin support for caching get requests using the
    memcache API.  This can be controlled via two class properties:

        caching: True to enable caching, False to disable

        cache_time: Time in seconds for results to be cached

        base_url: URL prefix expected on requests

        fetch_page_size: number of instances to return per get-all call
                         (note, App Engine has a builtin limit of 1000)

        authenticator: Authenticator which controls access to this service
                       (default allows any caller)

        authorizer: Authorizer which controls access to the data in this
                    service (default allows all access)

        output_content_types: content types which may be requested for
                              output.  the last value in the list is the
                              'default' type used when no type is requested
                              by the caller.  the only supported types are
                              currently JSON_CONTENT_TYPE and
                              XML_CONTENT_TYPE.

        include_docstring_in_schema: whether or not the docstring for a Model
                                     should be included in the schema.
                                     Defaults to False

        enable_delete_query: whether or not query based deletes are
                             supported.  Defaults to False

        enable_delete_all: whether or not 'delete all' queries are supported
                           (only used if enable_delete_query is True).
                           Defaults to False

        external_namespaces: a set of values which control how namespaces are
                             handled external to the handler.  The allowabled
                             values in the set are zero or more of READ and
                             WRITE.  If READ is specified, model namespaces
                             will be included in outgoing models.  If WRITE
                             is specified, callers are allowed to specify
                             namespaces in the url (otherwise, caller
                             specified namespaces will cause a 404).
                             Regardless of these settings, the programmer is
                             free to utilize namespaces interally to the
                             application.  If namespaces are being used for
                             multi-tenancy support, this property alone is
                             not sufficient protection against data leaks (as
                             namespace information is included in keys).
                             Secure multi-tenant implementations should have
                             an Authorizer which checks all model namespaces
                             (an example NamespaceAuthorizer is provided in
                             the wiki).  Defaults to hidden external
                             namespaces (empty set)

        enable_etags: whether or not etags (and related) headers are sent and
                      honored.  if enabled, 'If-Match' will be checked on sets
                      and 'If-None-Match' will be checked on gets.
                      Defaults to False

        simple_json_lists: whether or not json value lists should be
                           simplified.  by default, the json output closely
                           matches the xml output.  however, since json
                           already has an array structure, the default value
                           list output can be a bit cumbersone.  Enabling this
                           feature removes the additional 'item' key from
                           value lists.
                           Defaults to False

                           Default output:
                             'some_list' : { 'item' : [a, b, c]}
                           Simple output:
                             'some_list' : [a, b, c]
    """

    caching = False
    cache_time = 300
    base_url = ""
    fetch_page_size = 50
    authenticator = Authenticator()
    authorizer = Authorizer()
    if not COMPAT_WEBAPP2:
        # webapp picks the default as the last option
        output_content_types = [JSON_CONTENT_TYPE, XML_CONTENT_TYPE]
    else:
        # webapp2 picks the default as the first option
        output_content_types = [XML_CONTENT_TYPE, JSON_CONTENT_TYPE]
    include_docstring_in_schema = False
    enable_delete_query = False
    enable_delete_all = False
    external_namespaces = HIDDEN_EXT_NAMESPACES
    enable_etags = False
    simple_json_lists = False

    model_handlers = {}

    def __init__(self, request=None, response=None):
        if not COMPAT_WEBAPP2:
            super(Dispatcher, self).__init__()
        else:
            super(Dispatcher, self).__init__(request, response)

    @classmethod
    def add_models_from_module(cls, model_module, use_module_name=False,
                               exclude_model_types=None,
                               model_methods=ALL_MODEL_METHODS, recurse=False):
        """Adds all models from the given module to this request handler.
        The name of the Model class (with invalid characters converted to the
        '_' character) will be used as the REST path for Models of that type
        (optionally including the module name).

        REST paths which conflict with previously added paths will cause a
        KeyError.

        Args:

          model_module: a module instance or the name of a module instance
                        (e.g. use __name__ to add models from the current
                        module instance)
          use_module_name: True to include the name of the module as part of
                           the REST path for the Model, False to use the
                           Model name alone (this may be necessary if Models
                           with conflicting names are used from different
                           modules).
          exclude_model_types: optional list of Model types to be excluded
                               from the REST handler.
          model_methods: optional methods supported for the given model (one
                          or more of ['GET', 'POST', 'PUT',
                         'DELETE', 'GET_METADATA']), defaults to all methods
          recurse: True to recurse into sub-modules when searching for
                   Models, False otherwise

        """
        logging.info("adding models from module %s", model_module)
        if(not exclude_model_types):
            exclude_model_types = []
        if(isinstance(model_module, basestring)):
            model_module = __import__(model_module)
        module_name = ""
        if(use_module_name):
            module_name = get_type_name(model_module) + "."
        for obj_name in dir(model_module):
            obj = getattr(model_module, obj_name)
            if(isinstance(obj, types.ModuleType)):
                # only import "nested" modules, otherwise we get the whole
                # world and bad things happen
                if(recurse and get_type_name(obj).startswith(
                    get_type_name(model_module) + ".")):
                    cls.add_models_from_module(obj, use_module_name,
                                               exclude_model_types,
                                               model_methods, recurse)
            elif(isinstance(obj, type) and issubclass(obj, db.Model) and
                 (obj not in exclude_model_types)):
                model_name = module_name + get_type_name(obj)
                cls.add_model(model_name, obj, model_methods)

    @classmethod
    def add_models(cls, models, model_methods=ALL_MODEL_METHODS):
        """Adds the given models from the given dict to this request handler.
        The key (with invalid characters converted to the '_' character) will
        be used as the REST path for relevant Model value.

        REST paths which conflict with previously added paths will cause a
        KeyError.  Also, the path 'metadata' is reserved.

        Args:
          models: dict of REST path -> Model class (or a tuple of (Model
                  class, [allowed methods]) )

        """
        for model_name, model_type in models.iteritems():
            if is_list_type(model_type):
                # Assume we have format:
                # {model_name : (ModelClass, [model_method_1, model_method_2])}
                model_methods = model_type[1]
                model_type = model_type[0]

            cls.add_model(model_name, model_type, model_methods)

    @classmethod
    def add_model(cls, model_name, model_type,
                  model_methods=ALL_MODEL_METHODS):
        """Adds the given model to this request handler.  The name (with
        invalid characters converted to the '_' character) will be used as
        the REST path for relevant Model value.

        REST paths which conflict with previously added paths will cause a
        KeyError.  Also, the path 'metadata' is reserved.

        Args:
          model_name: the REST path for the given model
          model_type: the Model class

          model_methods: optional methods supported for the given model (one
                         or more of ['GET', 'POST', 'PUT', 'DELETE',
                         'GET_METADATA']), defaults to all methods
        """
        xml_name = convert_to_valid_xml_name(model_name)
        if(xml_name == METADATA_PATH):
            raise ValueError("cannot use name %s" % METADATA_PATH)
        if(model_name in cls.model_handlers):
            raise KeyError("name %s already used" % model_name)
        if(not issubclass(model_type, db.Model)):
            raise ValueError("given model type %s is not a subclass of Model" %
                             model_type)
        cls.model_handlers[xml_name] = ModelHandler(model_name, model_type,
                                                    model_methods)
        logging.info("added model %s with type %s for methods %s", model_name,
                     model_type, model_methods)

    ##
    # Error codes used in this handler:
    #
    # 200 -> okay
    # 204 -> noop (okay)
    # 400 -> bad req (bad data, invalid properties)
    # 404 -> not found (bad path)
    # 405 -> method not allowed (method not supported by model)
    ##

    def initialize(self, request, response):
        super(Dispatcher, self).initialize(request, response)
        if request:
            request.disp_query_params_ = None
        if response:
            response.disp_cache_resp_ = True
            response.disp_out_type_ = TEXT_CONTENT_TYPE

    def get(self, *_):
        """Does a REST get, optionally using memcache to cache results.  See
        get_impl() for more details."""

        self.authenticator.authenticate(self)

        if not self.caching:
            self.get_impl()
            return

        # attempt to return cached response
        cached_response = memcache.get(self.request.url)
        if cached_response:
            cached_response = pickle.loads(cached_response)
            # only use cache response if the requests match
            if cached_response.matches_request(self.request):
                if cached_response.is_not_modified(self):
                    self.not_modified()
                cached_response.write_output(self)
                return

        self.get_impl()

        # don't cache blobinfo content requests
        if self.response.disp_cache_resp_:
            cached_response = pickle.dumps(CachedResponse(self))
            if not memcache.set(self.request.url, cached_response,
                                self.cache_time):
                logging.warning("memcache set failed for %s",
                                self.request.url)

    def get_impl(self):
        """Actual implementation of REST get.  Gets metadata (types,
        schemas), or actual Model instances.

        '/metadata/*'          -> See get_metadata() for details
        '/<type>[?<query>]'    -> gets all Model instances of given type,
                                  optionally querying (200, 404)
        '/<type>/<key>'        -> gets Model instance with given key (200, 404)
        '/<type>/<key>/<prop>' -> gets a single property from the Model
                                  instance with given key (200, 404)

        """

        path = self.split_path(1)
        model_name = path.pop(0)

        if model_name == METADATA_PATH:
            out = self.get_metadata(path)

        elif(model_name == BLOBUPLOADRESULT_PATH):
            # this is the final call from a blobinfo upload
            self.response.disp_cache_resp_ = False
            path.append(BLOBUPLOADRESULT_PATH)
            model_name = path.pop(0)
            model_key = path.pop(0)
            self.update_impl(path, model_name, model_key, "POST", False)
            return

        else:
            model_handler = self.get_model_handler(model_name, "GET")
            model_name = model_handler.model_name

            list_props = {}
            if (len(path) > 0):
                model_key = path.pop(0)
                models = model_handler.get(model_key)

                self.authorizer.can_read(self, models)

                if (len(path) > 0):
                    # single property get
                    prop_name = path.pop(0)
                    prop_handler = model_handler.get_property_handler(
                        prop_name)
                    self.get_if_none_match(model_handler, models)
                    prop_value = prop_handler.get_value(models)
                    prop_handler.value_to_response(self, prop_name, prop_value,
                                                   path)
                    return

            else:
                models = self.get_all_impl(model_handler, list_props)

            if models is None:
                self.not_found()

            self.get_if_none_match(model_handler, models, list_props)

            out = self.models_to_xml(model_name, model_handler, models,
                                     list_props)

        self.write_output(out)

    def put(self, *_):
        """Does a REST put.

        '/<type>/<key>' -> completely replaces Model instance, returns key as
                           plain text (200, 400, 404)

        """

        self.authenticator.authenticate(self)

        path = self.split_path(1)
        model_name = path.pop(0)
        model_key = None
        if (len(path) > 0):
            model_key = path.pop(0)

        self.update_impl(path, model_name, model_key, "PUT", True)

    def post(self, *_):
        """Does a REST post, handles alternate HTTP methods specified via the
        'X-HTTP-Method-Override' header"""

        self.authenticator.authenticate(self)

        real_method = self.request.headers.get(METHOD_OVERRIDE_HEADER, None)
        if real_method:
            real_method = real_method.upper()
            if real_method == "PUT":
                self.put()
            elif real_method == "DELETE":
                self.delete()
            elif real_method == "POST":
                self.post_impl()
            elif real_method == "GET":
                self.get()
            else:
                self.error(405)
        else:
            self.post_impl()

    def post_impl(self, *_):
        """Actual implementation of REST post.

        '/<type>'       -> creates new Model instance, returns key as plain
                           text (200, 400, 404)
        '/<type>/<key>' -> partially updates Model instance, returns key as
                           plain text (200, 400, 404)

        """

        path = self.split_path(1)
        model_name = path.pop(0)

        model_key = None
        if (len(path) > 0):
            model_key = path.pop(0)

        self.update_impl(path, model_name, model_key, "POST", False)

    def update_impl(self, path, model_name, model_key, method_name,
                    is_replace):
        """Actual implementation of all Model update methods.
        Creates/updates/replaces Model instances as specified.  Writes the
        key of the modified Model as a plain text result.

        """

        model_handler = self.get_model_handler(model_name, method_name)
        model_name = model_handler.model_name

        is_list = False
        models = []

        if((not is_replace) and (len(path) > 0)):
            # single property update
            prop_name = path.pop(0)
            if(prop_name == KEY_PROPERTY_NAME):
                raise KeyError("Property %s is not modifiable" %
                               KEY_PROPERTY_NAME)
            model = model_handler.get(model_key)
            prop_handler = model_handler.get_property_handler(prop_name)
            prop_handler.value_from_request(self, model, path)
            models.append(model)

        else:

            doc = self.input_to_xml()

            model_els = [(model_key, doc.documentElement)]
            if(str(doc.documentElement.nodeName) == LIST_EL_NAME):
                is_list = True
                model_els = []
                for node in doc.documentElement.childNodes:
                    if(node.nodeType == node.ELEMENT_NODE):
                        model_els.append((MULTI_UPDATE_KEY, node))

            try:
                for model_el_key, model_el in model_els:
                    models.append(self.model_from_xml(
                        model_el, model_name, model_handler, model_el_key,
                        is_replace))
            except Exception:
                logging.exception("failed parsing model")
                raise DispatcherException(400)
            finally:
                doc.unlink()

        if is_list:
            models = self.authorizer.filter_write(self, models, is_replace)
        elif (len(models) > 0):
            self.authorizer.can_write(self, models[0], is_replace)

        self.update_if_match(model_handler, models)

        for model in models:
            ModelHandler.put(model)

        self.get_if_none_match(model_handler, models)

        # if input was not a list, convert single element models list back to
        # single element
        if(not is_list):
            models = models[0]

        # note, we specifically look in the query string (don't try to parse
        # the POST body)
        resp_type = self.get_query_param(QUERY_TYPE_PARAM)
        if (resp_type == QUERY_TYPE_FULL):
            self.write_output(self.models_to_xml(model_name, model_handler,
                                                 models))
        elif ((resp_type == QUERY_TYPE_STRUCTURED) or
              (resp_type == QUERY_TYPE_XML) or
              self.request_structured_output()):
            self.write_output(self.keys_to_xml(model_handler, models))
        else:
            self.write_output(self.keys_to_text(models))

    def request_structured_output(self):
        """Returns True if the request explicitly requested structured output
        (xml or json) via the accept header."""
        out_mime_type = unicode(self.request.accept)
        return ((out_mime_type == XML_CONTENT_TYPE) or
                (out_mime_type == JSON_CONTENT_TYPE))

    def delete(self, *_):
        """Does a REST delete.

        '/<type>/<key>'     -> delete Model instance w/ given key (200, 204)
        '/<type>[?<query>]' -> deletes all Model instances of given type,
                               optionally querying (200, 204)

        """

        self.authenticator.authenticate(self)

        path = self.split_path(1)
        model_name = path.pop(0)

        model_handler = self.get_model_handler(model_name, "DELETE", 204)
        model_name = model_handler.model_name

        model_key = None
        model_query = None

        if (len(path) > 0):
            model_key = path.pop(0)
        else:
            model_query = ModelQuery()
            model_query.parse(self, model_handler)

            # verify that this type of delete is supported
            if(not self.enable_delete_query):
                logging.warning("query based deletes are currently disabled,"
                                " see 'enable_delete_query' property")
                raise DispatcherException(404)
            elif((model_query.query_expr is None) and
                 (not self.enable_delete_all)):
                logging.warning("'delete all' deletes are currently disabled,"
                                " see 'enable_delete_all' property")
                raise DispatcherException(404)

        try:
            if (model_key is not None):
                model_key = db.Key(model_key)
                self.authorizer.can_delete(self, model_handler.model_type,
                                           model_key)
                self.update_if_match(model_handler, None, (model_key,))
                db.delete(model_key)
            else:
                model_query.query_expr = self.authorizer.check_delete_query(
                    self, model_query.query_expr, model_query.query_params)
                model_handler.delete_all(model_query)

        except Exception, ex:
            if(isinstance(ex, DispatcherException) and (ex.error_code == 412)):
                # we want to throw pre-condition failures
                raise ex
            logging.warning("delete failed", exc_info=1)
            self.error(204)

    def get_metadata(self, path):
        """Actual implementation of metadata retrieval.

        '/metadata'        -> gets list of all Model types
        '/metadata/<type>' -> gets XML Schema for the given type (200, 404)

        """

        model_name = None
        if (len(path) > 0):
            model_name = path.pop(0)

        impl = minidom.getDOMImplementation()
        doc = None

        try:
            if model_name:

                model_handler = self.get_model_handler(model_name,
                                                       "GET_METADATA")
                model_name = model_handler.model_name

                self.authorizer.can_read_metadata(self, model_name)

                doc = impl.createDocument(XSD_NS, XSD_SCHEMA_NAME, None)
                doc.documentElement.attributes[XSD_ATTR_XMLNS] = XSD_NS
                model_handler.write_xsd_metadata(doc.documentElement,
                                                 model_name)

            else:

                doc = impl.createDocument(None, TYPES_EL_NAME, None)
                types_el = doc.documentElement
                model_names = self.authorizer.filter_read_metadata(
                    self, list(self.model_handlers.iterkeys()))
                for model_name in model_names:
                    append_child(types_el, TYPE_EL_NAME, model_name)

            return self.doc_to_output(doc)

        finally:
            if doc:
                doc.unlink()

    def get_all_impl(self, model_handler, list_props):
        """Actual implementation of REST query.  Gets Model instances based
        on criteria specified in the query parameters.
        """

        model_query = ModelQuery()
        model_query.parse(self, model_handler)

        model_query.query_expr = self.authorizer.check_query(
            self, model_query.query_expr, model_query.query_params)

        models = model_handler.get_all(model_query)

        list_props[QUERY_OFFSET_PARAM] = model_query.next_fetch_offset

        models = self.authorizer.filter_read(self, models)

        return models

    def split_path(self, min_comps):
        """Returns the request path split into non-empty components."""
        path = self.request.path
        if(path.startswith(self.base_url)):
            path = path[len(self.base_url):]
        path = [i for i in path.split('/') if i]
        if(len(path) < min_comps):
            raise DispatcherException(404)
        return path

    def get_model_handler(self, model_name, method_name, failure_code=404):
        """Returns the ModelHandler with the given name, or None (and sets
        the error code given) if there is no handler with the given name."""

        # see if namespace was specified, e.g. "<ns>.<model_name>"
        ns_model_name = model_name.rpartition(".")
        if(ns_model_name[0]):
            # only set namespace if callers are allowed to
            if WRITE_EXT_NS not in self.external_namespaces:
                raise DispatcherException(404)
            namespace_manager.set_namespace(ns_model_name[0])
            # remove namespace from model name
            model_name = ns_model_name[2]

        try:
            model_handler = self.model_handlers[model_name]
        except KeyError:
            logging.error("invalid model name %s", model_name, exc_info=1)
            raise DispatcherException(failure_code)

        if method_name not in model_handler.model_methods:
            raise DispatcherException(405)

        return model_handler

    def doc_to_output(self, doc):
        """Returns the given xml doc serialized using the appropriate
        response format."""
        out_mime_type = self.request.accept.best_match(
            self.output_content_types)
        if(out_mime_type == JSON_CONTENT_TYPE):
            self.response.disp_out_type_ = JSON_CONTENT_TYPE
            return xml_to_json(doc)
        self.response.disp_out_type_ = XML_CONTENT_TYPE
        return doc.toxml(XML_ENCODING)

    def input_to_xml(self):
        """Returns the request doc converted into an xml doc."""
        content_type = self.request.headers.get(CONTENT_TYPE_HEADER, None)
        if((content_type != None) and
           content_type.startswith(JSON_CONTENT_TYPE)):
            return json_to_xml(self.request.body_file)
        return minidom.parse(self.request.body_file)

    def models_to_xml(self, model_name, model_handler, models,
                      list_props=None):
        """Returns a string of xml of the given models (may be list or single
        instance)."""
        blob_info_format = self.get_query_param(QUERY_BLOBINFO_PARAM,
                                                QUERY_BLOBINFO_TYPE_KEY)
        include_props = self.get_query_param(QUERY_INCLUDEPROPS_PARAM)
        if(include_props is not None):
            include_props = include_props.split(",")

        impl = minidom.getDOMImplementation()
        doc = None
        try:
            if is_list_type(models):
                doc = impl.createDocument(None, LIST_EL_NAME, None)
                list_el = mark_list_node(doc.documentElement)
                if((list_props is not None) and
                   (QUERY_OFFSET_PARAM in list_props)):
                    list_el.attributes[QUERY_OFFSET_PARAM] = (
                        list_props[QUERY_OFFSET_PARAM])

                for model in models:
                    model_el = append_child(list_el, model_name)
                    model_handler.write_xml_value(
                        model_el, model, blob_info_format, include_props)
            else:
                doc = impl.createDocument(None, model_name, None)
                model_handler.write_xml_value(doc.documentElement, models,
                                              blob_info_format, include_props)

            return self.doc_to_output(doc)
        finally:
            if doc:
                doc.unlink()

    def get_if_none_match(self, model_handler, models, list_props=None):
        """Handles the 'If-None-Match' header for retrieving data, either
        setting the outgoing ETag header or returning the not modified
        response code as appropriate.  Does nothing if etag support is not
        enabled."""
        if not self.enable_etags:
            return

        model_hash = self.models_to_hash(model_handler, models, list_props)
        if model_hash in self.request.if_none_match:
            self.not_modified()
        self.response.headers[ETAG_HEADER] = '"%s"' % model_hash

    def update_if_match(self, model_handler, models, model_keys=None):
        """Handles the 'If-Match' header for modifying data, either allowing
        the update to proceed or returning the precondition failed response
        code as appropriate.  Does nothing if etag support is not enabled."""
        if not self.enable_etags:
            return

        if('*' not in self.request.if_match):
            # caller provided an if-match header (which may be a per-model
            # list or an all-collection value)

            if(model_keys is not None):
                # first need to convert keys to models
                models = []
                for model_key in model_keys:
                    model = model_handler.get(model_key)
                    if model:
                        models.append(model)

            if(self.models_to_hash(model_handler, models) in
               self.request.if_match):
                # provided per-collection header, which matches
                return

            for model in models:
                if(model_hash_to_str(ModelHandler.hash_model(model)) not in
                   self.request.if_match):
                    # provided per-model header, which does not match
                    self.is_modified()

        elif(models is not None):
            # see if caller provided per-model etags in the models themselves
            # (as attributes)
            for model in models:
                if(hasattr(model, "in_model_hash_") and
                   (model.in_model_hash_ !=
                    model_hash_to_str(ModelHandler.hash_model(model)))):
                    # provided per-model attribute, which does not match
                    self.is_modified()

    def models_to_hash(self, model_handler, models, list_props=None):
        """Returns an aggregate hash for a collection of models, including any
        query offset parameter if available."""
        model_hash = 0
        if is_list_type(models):
            if((list_props is not None) and
               (QUERY_OFFSET_PARAM in list_props)):
                model_hash = model_hash ^ hash(list_props[QUERY_OFFSET_PARAM])
            for model in models:
                model_hash = model_hash ^ ModelHandler.hash_model(model)
        else:
            model_hash = model_hash ^ ModelHandler.hash_model(models)

        return model_hash_to_str(model_hash)

    def keys_to_xml(self, model_handler, models):
        """Returns a string of xml of the keys of the given models (may be
        list or single instance)."""
        impl = minidom.getDOMImplementation()
        doc = None
        try:
            if is_list_type(models):
                doc = impl.createDocument(None, LIST_EL_NAME, None)
                list_el = mark_list_node(doc.documentElement)

                for model in models:
                    append_child(list_el, KEY_PROPERTY_NAME,
                                 model_handler.key_handler.get_value_as_string(
                                     model))
            else:
                doc = impl.createDocument(None, KEY_PROPERTY_NAME, None)
                doc.documentElement.appendChild(doc.createTextNode(
                    model_handler.key_handler.get_value_as_string(models)))

            return self.doc_to_output(doc)
        finally:
            if doc:
                doc.unlink()

    def keys_to_text(self, models):
        """Returns a string of text of the keys of the given models (may be
        list or single instance)."""
        if(not is_list_type(models)):
            models = [models]
        return unicode(",".join([str(model.key()) for model in models]))

    def model_from_xml(self, model_el, model_name, model_handler, key,
                       is_replace):
        """Returns a model instance updated from the given model xml
        element."""
        if(model_name != str(model_el.nodeName)):
            raise TypeError("wrong model name, found '%s', expected '%s'" %
                            (model_el.nodeName, model_name))

        props = model_handler.read_xml_value(model_el)

        given_key = props.pop(KEY_PROPERTY_NAME, None)

        if(key is MULTI_UPDATE_KEY):
            if(given_key):
                key = str(given_key)
            else:
                key = None

        new_model = False
        if(key):
            key = db.Key(key.strip())
            if(given_key and (given_key != key)):
                raise ValueError(
                    "key in data %s does not match request key %s" %
                    (given_key, key))
            model = model_handler.get(key)

            if(is_replace):
                for prop_name, prop_type in model.properties().iteritems():
                    if(prop_name not in props):
                        setattr(model, prop_name, prop_type.default_value())
                for prop_name in model.dynamic_properties():
                    delattr(model, prop_name)

            for prop_name, prop_value in props.iteritems():
                setattr(model, prop_name, prop_value)

        else:
            new_model = True
            model = model_handler.create(props)

        # check for model specific etag attribute (ignore for new model)
        if(self.enable_etags and (not new_model) and
           (model_el.attributes.get(ETAG_ATTR_NAME, None) is not None)):
            model.in_model_hash_ = str(
                model_el.attributes[ETAG_ATTR_NAME].value)

        return model

    def write_output(self, out):
        """Writes the output to the response."""
        if out:
            content_type = self.response.disp_out_type_
            out_suffix = None
            if(content_type == JSON_CONTENT_TYPE):
                # check for json callback
                callback = self.get_query_param(QUERY_CALLBACK_PARAM)
                if callback:
                    self.response.out.write(callback)
                    self.response.out.write("(")
                    out_suffix = ");"

            self.response.headers[CONTENT_TYPE_HEADER] = content_type
            self.response.out.write(out)
            if out_suffix:
                self.response.out.write(out_suffix)

    def serve_blob(self, blob_info):
        """Serves a BlobInfo response."""
        self.response.clear()
        self.response.disp_cache_resp_ = False

        content_type_preferred = None
        if blob_info:
            # return actual blob
            range_header = self.request.headers.get(RANGE_HEADER, None)
            if range_header is not None:
                self.response.headers[blobstore.BLOB_RANGE_HEADER] = (
                    range_header)
            self.response.headers[blobstore.BLOB_KEY_HEADER] = (
                str(blob_info.key()))
            content_type_preferred = blob_info.content_type

        self.set_response_content_type(BINARY_CONTENT_TYPE,
                                       content_type_preferred)

    def upload_blob(self, path, model, blob_prop_name):
        """Handles a BlobInfo upload to the property with the given name of
        the given model instance."""

        if(len(path) > 0):
            if(path.pop(0) != BLOBUPLOADRESULT_PATH):
                raise DispatcherException(404)

            # final leg of a blob upload, no modifications left to make, just
            # return model result
            return

        # set blobinfo contents
        content_type = self.request.headers.get(CONTENT_TYPE_HEADER, None)
        if(not content_type.startswith(FORMDATA_CONTENT_TYPE)):

            # pre-authorize the upload
            self.authorizer.can_write_blobinfo(self, model, blob_prop_name)

            # need to return upload form
            redirect_url = self.request.path
            if self.request.query_string:
                redirect_url += "?" + self.request.query_string
            form_url = blobstore.create_upload_url(redirect_url)
            self.response.out.write('<html><body>')
            self.response.out.write(
                '<form action="%s" method="POST" enctype="%s">' %
                (form_url, FORMDATA_CONTENT_TYPE))
            self.response.out.write(
                """Upload File: <input type="file" name="file"><br> """
                """<input type="submit" name="submit" value="Submit"> """
                """</form></body></html>""")
            raise DispatcherException()

        else:

            # upload completed, update the model
            blob_key = None
            for key, value in self.request.params.items():
                if((key == "file") and isinstance(value, cgi.FieldStorage)):
                    if 'blob-key' in value.type_options:
                        blob_key = blobstore.parse_blob_info(value).key()

            if blob_key is None:
                raise ValueError("Blob upload failed")

            setattr(model, blob_prop_name, blob_key)

            # authorize the update, post upload.  we need to do this here,
            # because we have to return a redirect now (the final result is
            # not returned until after the redirect)
            self.authorizer.can_write(self, model, False)

            ModelHandler.put(model)

            # redirect will be a GET, so we need to send the caller to a
            # special url, so they can get output which looks like what would
            # normally result from an update call
            result_url = (self.base_url + "/" + BLOBUPLOADRESULT_PATH +
                          self.request.path[len(self.base_url):])

            if self.request.query_string:
                result_url += "?" + self.request.query_string

            self.redirect(result_url)
            raise DispatcherException()

    def handle_exception(self, exception, debug_mode):
        if(isinstance(exception, DispatcherException)):
            # if None, assume thrower has configured the response appropriately
            if(exception.error_code is not None):
                if(exception.error_code < 400):
                    self.response.clear()
                    self.response.set_status(exception.error_code)
                else:
                    self.error(exception.error_code)
        else:
            super(Dispatcher, self).handle_exception(exception, debug_mode)

    def get_query_params(self):
        """Returns the parsed request url query params as a dict."""
        # lazy (re)parse query params
        if(self.request.disp_query_params_ is None):
            self.request.disp_query_params_ = cgi.parse_qs(
                self.request.query_string)
        return self.request.disp_query_params_

    def get_query_param(self, key, default=None):
        """Returns the request url query param for the given key, defaulting
        to the given default if not found."""
        value = self.get_query_params().get(key, None)
        if(value is None):
            return default
        return value[0]

    def set_response_content_type(self, content_type_default,
                                  content_type_preferred=None):
        """Sets the response content-type header based on the request and the
        given info."""
        content_type = content_type_preferred
        if((not content_type) or (content_type.find("*") >= 0)):
            content_type = self.request.accept.best_matches()[0]
            if((not content_type) or (content_type.find("*") >= 0)):
                content_type = content_type_default
        self.response.headers[CONTENT_TYPE_HEADER] = content_type
        self.response.disp_out_type_ = content_type

    def forbidden(self):
        """Convenience method which raises a DispatcherException with a 403
        error code."""
        raise DispatcherException(403)

    def not_found(self):
        """Convenience method which raises a DispatcherException with a 404
        error code."""
        raise DispatcherException(404)

    def not_modified(self):
        """Convenience method which raises a DispatcherException with a 304
        status code."""
        self.response.headers[CONTENT_TYPE_HEADER] = TEXT_CONTENT_TYPE
        raise DispatcherException(304)

    def is_modified(self):
        """Convenience method which raises a DispatcherException with a 412
        error code."""
        raise DispatcherException(412)
