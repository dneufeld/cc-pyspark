import ujson as json

from urllib.parse import urlparse

from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector

from pyspark.sql.types import StructType, StructField, StringType, LongType

from sparkcc import CCSparkJob


class ShopifyMetadataJob(CCSparkJob):
    """ Collect shopify metadata from WARC response records
        (WARC and WAT is allowed as input)"""

    name = "ShopifyMetadata"

    output_schema = StructType([
          StructField("host", StringType(), True),
          StructField("ip", StringType(), True),
          StructField("http_headers", StringType(), True)
#          StructField("payload", StringType(), True)
    ])
    
    response_no_ip_address = '(no IP address)'
    response_no_host = '(no host name)'

    def process_record(self, record):
        ip_address = None
        url = None
        http_headers = ""

        if self.is_wat_json_record(record):
            # WAT (response) record
            record = json.loads(self.get_payload_stream(record).read())
            try:
                warc_header = record['Envelope']['WARC-Header-Metadata']
                if warc_header['WARC-Type'] != 'response':
                    # WAT request or metadata records
                    return
                if 'WARC-IP-Address' in warc_header:
                    ip_address = warc_header['WARC-IP-Address']
                    url = warc_header['WARC-Target-URI']
                else:
                    # WAT metadata records
                    return
            except KeyError:
                pass
        elif self.is_response_record(record):
            # WARC response record
            ip_address = self.get_warc_header(record, 'WARC-IP-Address')
            if ip_address.startswith("23.227.38"):
                url = self.get_warc_header(record, 'WARC-Target-URI')
                for (name, value) in self.get_http_headers(record):
                    http_headers = http_headers + name + ":" + value
#                if not self.is_html(record):
#                   # skip non-HTML or unknown content types
#                   return
#                payload = self.get_payload_stream(record).read().decode('utf-8')
            else:
                return
        else:
            # warcinfo, request, non-WAT metadata records
            return

        if not ip_address or ip_address == '':
            ip_address = ShopifyMetadataJob.response_no_ip_address

            host_name = ShopifyMetadataJob.response_no_host
        if url:
            try:
                host_name = urlparse(url).hostname
            except:
                pass

        yield (host_name, ip_address, http_headers)


if __name__ == "__main__":
    job = ShopifyMetadataJob()
    job.run()
