from bigquery_uploader.datauploader import Datauploader, bq_streaming_cloud_function_table_schema
import sys
from pympler.asizeof import asizeof


def upload_bigquery_chunks(data_list, bigquery_extractor, bq_table_id, chunk_mb=9):
    def getChunks(data_list, n=20):
        for i in range(0, len(data_list), n):
            yield data_list[i: i + n]

    # avg_size_dd = sys.getsizeof(data_list)/len(data_list)
    avg_size_dd = asizeof(data_list[0])*3
    print(avg_size_dd)
    num_chunks = int(chunk_mb * (10 ** 6)/avg_size_dd)
    data_chunks = getChunks(data_list, num_chunks)
    for data in data_chunks:
        bigquery_extractor.client.insert_rows_json(bq_table_id, data)


def bq_data_uploader_stream(request):
    bq_project_id = 'long-grin-524'
    bq_dataset = 'Ronnie_Testing_Dataset'
    bq_table_id = "bq_streaming_cloud_function_table_3"

    data_list = list()
    # 1MB = 1 * (10**6)
    MBs = 50
    while sys.getsizeof(data_list) < MBs * (10 ** 6):
        data_list.append({'campaign_name': 'Ronnie Joshua', 'campaign_id': 123456789})

    bq_extractor = Datauploader(bq_project_id)
    bq_extractor.create_table(bq_dataset, bq_table_id, bq_streaming_cloud_function_table_schema)
    bq_table_id = '{0}.{1}.{2}'.format(bq_project_id, bq_dataset, bq_table_id)
    upload_bigquery_chunks(data_list, bq_extractor, bq_table_id)
