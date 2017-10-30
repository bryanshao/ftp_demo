import os

from parsekit.steps.output import OutputCSV
from parsekit.errors import ParseDefinitionError

from river import RiverClient


class OutputRiver(OutputCSV):

    river_client = None
    collection = None

    def setup(self, metadata, *args, **kwargs):
        # get River connection parameters from environment
        river_base_url = os.getenv('RIVER_BASE_URL')
        river_api_key = os.getenv('RIVER_API_KEY')
        # instantiate river client if not instantiated
        self.db_boundary = metadata.get_closest('db_boundary')
        if not self.db_boundary:
            raise ParseDefinitionError(
                "River output requires `db_boundary` key in Parsefile.")
        if self.river_client is not None:
            self.log.debug("River collection for this parser already exists.")
            return
        river_client = RiverClient(river_base_url, river_api_key)
        self.__class__.river_client = river_client
        self.__class__.collection = river_client.create_collection(self.db_boundary)
        self.log.info("Created River collection with id: '%s', and name: '%s' "
                      "for current run.",
                      self.collection.id, self.collection.name)

    def teardown(self, reason, *args, **kwargs):
        self.log.debug("Closing all outputs from OutputCSV.")
        for datapath in self.outputs:
            self.outputs[datapath]['fh'].close()
        if reason != 'complete':
            return
        for datapath, output in self.outputs.iteritems():
            output['fh'].close()
            dataset = self.collection.create_dataset(
                metadata={'datapath': datapath},
                schema=output['schema'].serialize(),
                name=output['dataset_name'],
                source={'format': 'csv', 'header': True})
            self.log.debug(
                "Uploading dataset id: '%s' for datapath: '%s'to '%s'.",
                dataset.id, datapath, dataset.upload_url)
            try:
                dataset.upload_data(output['path'])
            except Exception as e:
                self.log.error("Upload faield due to: %s", str(e))
                exit(-1)
            dataset.begin_copy()
