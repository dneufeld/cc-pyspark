import ujson as json

from urllib.parse import urlparse

from datetime import datetime

from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from sparkcc import CCSparkJob


class ShopifyMetadataJob(CCSparkJob):
    """ Collect shopify metadata from WARC response records
        (WARC and WAT is allowed as input)"""

    name = "ShopifyMetadata"

    output_schema = StructType([
          StructField("shop_id", StringType(), True),
          StructField("host", StringType(), True),
          StructField("ip", StringType(), True),
          StructField("date", TimestampType(), True),
          StructField("url", StringType(), True),
          StructField("http_headers", StringType(), True),
          StructField("text", StringType(), True)
    ])
    
    response_no_ip_address = '(no IP address)'
    response_no_host = '(no host name)'

    def html_to_text(self, page, record):
        try:
            encoding = self.get_warc_header(record, 'WARC-Identified-Content-Charset')
            if not encoding:
                for encoding in EncodingDetector(page, is_html=True).encodings:
                    # take the first detected encoding
                    break
            soup = BeautifulSoup(page, 'lxml', from_encoding=encoding)
            return soup.get_text()
        except Exception as e:
            self.get_logger().error("Error converting HTML to text for {}: {}",
                                    self.get_warc_header(record, 'WARC-Target-URI'), e)
            self.records_parsing_failed.add(1)
            return ''

    def process_record(self, record):
        shop_id = None
        ip_address = None
        url = None
        http_headers = ""
        date = None

        if self.is_response_record(record):
            # WARC response record
            ip_address = self.get_warc_header(record, 'WARC-IP-Address')
            if ip_address.startswith("23.227.38"):
                url = self.get_warc_header(record, 'WARC-Target-URI')

                date_string = self.get_warc_header(record, 'WARC-Date')
                date = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
               

                http_headers = self.get_http_headers(record)
                for (name, value) in http_headers:
                    if name == 'X-Sorting-Hat-PodId':
                        if value == '':
                            pass
                        else:
                            shop_id = value

                http_headers = str(http_headers)

                if not self.is_html(record):
                   # skip non-HTML or unknown content types
                   return
                page = self.get_payload_stream(record).read()
                text = self.html_to_text(page, record)
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

        yield (shop_id, host_name, ip_address, date, url, http_headers, text)


if __name__ == "__main__":
    job = ShopifyMetadataJob()
    job.run()
