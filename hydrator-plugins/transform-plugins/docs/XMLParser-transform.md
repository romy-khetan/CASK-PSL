# XML Parser Transform

Description
-----------
The XML Parser uses XPath to extract field from a complex XML Event. This is generally used in conjunction with the
XML Reader Batch Source. The XML Source Reader will provide individual events to the XML Parser and the XML Parser is
responsible for extracting fields from the event and mapping them to output schema.

Use Case
--------
1.  User should be able to specify the input field that should be considered as source of XML event or record.
2.  User is able to specify XML encoding (default is UTF-8)
3.  The Plugin should ignore comments in XML
3.  User is able to specify a collection of XPath to output field name mapping
      a.  User is able to extract values from Attribute (as supported by XPath)
      b.  User is NOT able to XPaths that are arrays. It should be runtime error.
6.  User is able to specify the output field types and the plugin performs appropriate conversions
7.  User is able to specify what should happen when there is error in processing
      a.  User can specify that the error record should be ignored
      b.  User can specify that upon error it should stop processing
      c.  User can specify that all the error records should be written to separate dataset

Properties
----------

**input:** Specifies the field in input that should be considered as source of XML event or record.

**encoding:** Specifies XML encoding type(default is UTF-8).

**xpathMappings:** Specifies a mapping from XPath of XML record to field name.

**fieldTypeMapping:** Specifies the field name as specified in xpathMappings and its corresponding data type.
The data type can be of following types- boolean, int, long, float, double, bytes, string

**processOnError:** Specifies what happens in case of error.
                     1. Ignore the error record
                     2. Stop processing upon encoutering error
                     3. Write error record to different dataset

Example
-------

This example parses the xml record received in the the "body" field of the structured record, according to the
xpathMappings specified, for each field name.
The type output schema will be created, using the type specified for each field in "fieldTypeMapping".

       {
            "name": "XMLParser",
            "plugin": {
                "name": "XMLParser",
                "type": "transform",
                "label": "XMLParser",
                "properties": {
                    "encoding": "UTF-8",
                     "processOnError": "UTF-8",
                      "xpathMappings": "category://book/@category,title://book/title,year:/bookstore/book[price>35.00]
                      /year,price:/bookstore/book[price>35.00]/price,subcategory://book/subcategory",
                      "fieldTypeMapping": "category:string,title:string,year:int,price:double,subcategory:string",
                      "input": "body"
                }
            }
       }