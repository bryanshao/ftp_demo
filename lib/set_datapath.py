import parsekit


class SetDatapath(parsekit.Step):

    datapath = parsekit.Argument(
        "A table's datapath.",
        required=True,
        type=str)

    def run(self, record, metadata):
        metadata['datapath'] = self.options.datapath
        return record, metadata
