import json
import sys
import pandas
import os

from piicatcher.catalog import Store
from piicatcher.piitypes import PiiTypeEncoder


class FileStore(Store):
    @classmethod
    def save_schemas(cls, explorer):
        if explorer.catalog["file"] is not None:
            json.dump(
                explorer.get_dict(),
                explorer.catalog["file"],
                sort_keys=True,
                indent=2,
                cls=PiiTypeEncoder,
            )
        else:
            json.dump(
                explorer.get_dict(),
                sys.stdout,
                sort_keys=True,
                indent=2,
                cls=PiiTypeEncoder,
            )

    @classmethod
    def save_schemas_csv(cls, explorer, ns, headers):
        # Save csv to current working dir
        cwd = os.getcwd()
        csv_path = cwd + f"/{ns.database}.csv"
        print("Saving output to  %s" % csv_path)
        df = pandas.DataFrame(explorer.get_tabular(ns.list_all), columns=headers)
        df.insert(0, "database", ns.database)
        df.to_csv(csv_path, index=False, header=True)
