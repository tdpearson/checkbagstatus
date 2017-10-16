import requests
from json import loads, dumps
from lxml import etree
from itertools import compress
from secrets import alma_key

base_url = "https://cc.lib.ou.edu"
digital_object_url = "{0}/api/catalog/data/catalog/digital_objects".format(base_url)
search_url = "{0}/.json?query={{\"filter\":{{\"project\":\"private\",\"bag\":{{\"$regex\":\"^share*\"}}}}}}".format(digital_object_url)
# search string   {"filter":{"project":"private","bag":{"$regex":"^share*"}}}
alma_url = "https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{0}?expand=None&apikey={1}"


xpath_lookup = {
    "Title": "record/datafield[@tag=245]",
    "Author": "record/datafield[@tag=100]",
    "Publish Year": "record/datafield[@tag=264]|record/datafield[@tag=260]",
    "Thesis/Diss Tag": "record/datafield[@tag=502]",
    "School": "record/datafield[@tag=690]",
    "Subject Heading": "record/datafield[@tag=650]"
}


def get_bag_name(val):
    return val['bag'] 


def get_mmsid(val):
    return val.split("_")[-1]


def get_bags(url):
    """ iterate over pages of search results yielding bag metadata """

    def all_results(url):
        # TODO: handle if site or url does not exist
        data = loads(requests.get(url).content)
        yield data['results']
        if data['next'] is not None:
            for result in all_results(data['next']):
                yield result
    for result in all_results(url):
        for bag in result:
            yield bag


def get_bib_record(mmsid):
    return requests.get(alma_url.format(mmsid, alma_key)).content


def missing_fields(bib_record):
    root = etree.fromstring(bib_record)

    def missing_or_blank(xpath_val):
        results = root.xpath(xpath_val)
        if len(results) > 0:
            return results[0].text == ""
        else:
            return True
    missing = map(missing_or_blank, xpath_lookup.values())
    return list(compress(xpath_lookup.keys(), missing))


def suppress_publishing(bib_record):
    """ checks permission status to publish """
    root = etree.fromstring(bib_record)
    return root.xpath('//suppress_from_publishing')[0].text.upper() == "TRUE"


def get_marc_from_bib(bib_record):
    """ returns marc xml from bib record string"""
    record = etree.fromstring(bib_record).find("record")
    record.attrib['xmlns'] = "http://www.loc.gov/MARC21/slim"
    return etree.ElementTree(record)


def marc_xml_to_db_xml(marc_xml):
    """ returns dublin core xml from marc xml """
    marc2dc_xslt = etree.parse('xlst/MARC21slim2RDFDC.xsl')
    transform = etree.XSLT(marc2dc_xslt)
    return transform(marc_xml)


def validate_marc(marc_xml):
    with open('xlst/MARC21slim.xsd') as f:
        schema = etree.XMLSchema(etree.fromstring(f.read()))
    parser = etree.XMLParser(schema=schema)
    return etree.fromstring(etree.tostring(marc_xml), parser)


def bib_to_dc(bib_record):
    return marc_xml_to_db_xml(validate_marc(get_marc_from_bib(bib_record)))


bag_names = map(get_bag_name, get_bags(search_url))
mmsids = map(get_mmsid, bag_names)
bib_records = map(get_bib_record, mmsids)
missing = map(missing_fields, bib_records)
results = zip(bag_names, missing)


for index, result in enumerate(results):
    bag, missing = result
    if missing == []:
        if not suppress_publishing(bib_records[index]):
            dc = bib_to_dc(bib_records[index])
            # TODO:
            # generate saf
            # submit specified files(*.pdf, Abstract.txt, Committee.txt) and metadata (dc and saf)
    else:
        print(result)
        # TODO:
        # send email to creator and owner notifying of missing fields
