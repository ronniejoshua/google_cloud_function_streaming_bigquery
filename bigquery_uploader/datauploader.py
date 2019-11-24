from google.api_core import exceptions
from google.cloud.exceptions import NotFound
from google.cloud import storage
from google.cloud import bigquery


class Datauploader(object):

    def __init__(self, project_id):
        # Construct a BigQuery client object.
        self.client = bigquery.Client()
        self.project_id = project_id

    @staticmethod
    def create_bigquery_load_job(my_table_schema, gs_file_format='JSON', skip_leading_n_row=0):
        """
        :param table_schema:
        :param gs_file_format:
        :param skip_leading_n_row:
        :return:
        """
        job_config = bigquery.LoadJobConfig()
        job_config.schema = my_table_schema
        if gs_file_format == "JSON":
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        if gs_file_format == "CSV":
            job_config.skip_leading_rows = skip_leading_n_row
            job_config.source_format = bigquery.SourceFormat.CSV
        if gs_file_format == "LOCAL_CSV":
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.skip_leading_rows = 1
            job_config.autodetect = True
        if gs_file_format == "ORC":
            job_config.source_format = bigquery.SourceFormat.ORC
        if gs_file_format == "PARQUET":
            job_config.source_format = bigquery.SourceFormat.PARQUET
        return job_config

    def dataset_exists(self, my_dataset_id):
        """Return if a dataset exists.
        Args:
            client (google.cloud.bigquery.client.Client):
                A client to connect to the BigQuery API.
            dataset_reference (google.cloud.bigquery.dataset.DatasetReference):
                A reference to the dataset to look for.
        Returns:
            bool: ``True`` if the dataset exists, ``False`` otherwise.
        """
        dataset_id = my_dataset_id
        dataset_ref = self.client.dataset(dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
            return True
        except NotFound:
            return False

    def table_exists(self, my_dataset_id, my_table_id):
        """Return if a table exists.
        Args:
            client (google.cloud.bigquery.client.Client):
                A client to connect to the BigQuery API.
            table_reference (google.cloud.bigquery.table.TableReference):
                A reference to the table to look for.
        Returns:
            bool: ``True`` if the table exists, ``False`` otherwise.
        """
        dataset_id = my_dataset_id
        table_id = my_table_id
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        try:
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False

    def create_dataset(self, my_dataset_id, location="US", description=None):
        """
        https://cloud.google.com/bigquery/docs/datasets
        """
        try:
            dataset_id = "{0}.{1}".format(self.project_id, my_dataset_id)
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = location
            dataset.description = description
            dataset = self.client.create_dataset(dataset)
            print("Created dataset {}.{}".format(self.client.project, dataset.dataset_id))
        except exceptions.Conflict:
            print("Error: {0} Already Exists".format(dataset_id))

    def delete_dataset(self, my_dataset_id):
        """
        https://cloud.google.com/bigquery/docs/managing-datasets#delete-dataset
        """
        try:
            dataset_id = "{0}.{1}".format(self.project_id, my_dataset_id)
            self.client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
            print("Deleted dataset '{}'.".format(dataset_id))
        except exceptions.BadRequest:
            print("Couldn't Delete Dataset, Delete the Tables First.")

    def create_table(self, my_dataset_id, my_table_id, table_schema, req_partition=False, partition_by=None,
                     description=None):
        """

        :param my_dataset_id:
        :param my_table_id:
        :param table_schema:
        :param req_partition:
        :param partition_by:
        :param description:
        :return:
        """
        dataset_ref = self.client.dataset(my_dataset_id)
        table_ref = dataset_ref.table(my_table_id)

        # Checking if table exists & Creating Table
        if self.table_exists(my_dataset_id, my_table_id):
            print('Table Already Exists - Appending Data')
            return None
        else:
            print("Table Doesn't Exists - Creating New Table")
            table = bigquery.Table(table_ref, schema=table_schema)
            table.description = description

            if req_partition:
                table_partition_field = partition_by
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=table_partition_field
                )
                table = self.client.create_table(table)
                print(
                    "Created table {}.{}.{} partitioned by {} ".format(
                        table.project,
                        table.dataset_id,
                        table.table_id,
                        table.time_partitioning.field
                    )
                )
            else:
                table = self.client.create_table(table)
                print("Created table {}.{}.{}".format(
                    table.project,
                    table.dataset_id,
                    table.table_id
                ))

        table_out = self.client.get_table(table_ref)
        return table_out

    def list_tables(self, my_dataset_id):
        """

        :param my_dataset_id:
        :return:
        """
        dataset_id = "{0}.{1}".format(self.project_id, my_dataset_id)
        tables = self.client.list_tables(dataset_id)
        print("Tables contained in '{}':".format(dataset_id))
        for table in tables:
            print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    def get_table(self, my_dataset_id, my_table_id):
        """

        :param my_dataset_id:
        :param my_table_id:
        :return:
        """
        table_id = '{}.{}.{}'.format(self.project_id, my_dataset_id, my_table_id)
        table = self.client.get_table(table_id)
        print("Got table '{}.{}.{}'.".format(table.project, table.dataset_id, table.table_id))
        print("Table schema: {}".format(table.schema))
        print("Table description: {}".format(table.description))
        print("Table has {} rows".format(table.num_rows))

    def upload_data_gcs(self, my_gcs_bucket, my_blob_name, my_local_file_path_plus_name):
        storage_client = storage.Client(self.project_id)
        bucket = storage_client.get_bucket(my_gcs_bucket)
        blob = bucket.blob(my_blob_name)

        """
        https://github.com/hackersandslackers/googlecloud-storage-tutorial/blob/master/main.py

        # Uploading string of text
        blob.upload_from_string('this is test content!')

        # Uploading from a local file using open()
        with open('photo.jpg', 'rb') as photo:
            blob.upload_from_file(photo)

        # Uploading from local file without open()
            blob.upload_from_filename('photo.jpg')
        """

        blob.upload_from_filename(my_local_file_path_plus_name)
        uri = "gs://{}/{}".format(my_gcs_bucket, my_blob_name)
        print(uri)
        return uri

    def upload_table_from_uri(self, my_uri, my_job_config, my_dataset_id, my_table_id):
        dataset_id = my_dataset_id
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(my_table_id)

        load_job = self.client.load_table_from_uri(
            my_uri,
            table_ref,
            location="US",
            job_config=my_job_config
        )  # API request

        print("Starting job {}".format(load_job.job_id))
        load_job.result()  # Waits for table load to complete.
        print("Job finished.")
        print("Loaded {} rows into {}:{}.".format(load_job.output_rows, dataset_id, my_table_id))
        destination_table = self.client.get_table(dataset_ref.table(my_table_id))
        print("Total {} rows.".format(destination_table.num_rows))

    def upload_table_from_file(self, my_filename_plus_path, my_dataset_id, my_table_id, my_job_config):
        filename = my_filename_plus_path
        dataset_id = my_dataset_id
        table_id = my_table_id
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        with open(filename, "rb") as source_file:
            job = self.client.load_table_from_file(
                source_file,
                table_ref,
                job_config=my_job_config
            )
        job.result()  # Waits for table load to complete.
        print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
        destination_table = self.client.get_table(dataset_ref.table(my_table_id))
        print("Total {} rows.".format(destination_table.num_rows))

    def delete_table(self, my_dataset_id, my_table_id):
        """

        :param my_dataset_id:
        :param my_table_id:
        :return:
        """
        table_id = '{}.{}.{}'.format(self.project_id, my_dataset_id, my_table_id)
        try:
            self.client.delete_table(table_id, not_found_ok=False)
            print("Deleted table '{}'.".format(table_id))
        except exceptions.NotFound:
            print("Couldn't Delete Table, Table Doesn't Exists.")

    @staticmethod
    def delete_blob(my_gcs_bucket, my_blob_name):
        """
        Deletes a blob from the bucket.
        :param my_gcs_bucket:
        :param my_blob_name:
        :return:
        """
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(my_gcs_bucket)
        blob = bucket.blob(my_blob_name)
        blob.delete()
        print('Blob {} deleted.'.format(my_blob_name))


bq_streaming_cloud_function_table_schema = [
    bigquery.SchemaField('campaign_name', 'STRING'),
    bigquery.SchemaField('campaign_id', 'INT64')
]
