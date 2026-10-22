# Local data and runtime evidence

Source data, uploaded brand material, private campaign images, transcripts, generated
campaign artifacts and cloud diagnostic receipts are deliberately not included in
this source-code repository. They remain in their authorized local or cloud stores.

Some configuration and historical checkpoint documents reference these files.
Those references describe the original development environment; the files are not
available merely by cloning this repository. Obtain the required inputs through
an authorized source before running data-dependent workflows.

The `src/ingestion/` modules contain the ingestion and synthetic-data generation
code. Inspect a module's configuration and entry point before running it: some
commands provision resources or write to Google Cloud. Do not treat diagnostic
runners under `src/validation/` as an offline test suite.
