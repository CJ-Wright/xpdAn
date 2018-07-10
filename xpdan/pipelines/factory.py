from bluesky.callbacks.core import CallbackBase


ex_pipeline_func_dict = {
    "PDF": {
        "func": pdf_pipeline_factory,
        "input": "raw_input",
        "cache_mod": pdf_cache_mod,
    }
}


class PipelineFactory(CallbackBase):
    def __init__(self, pipeline_func_dict):
        self.pipelines = pipeline_func_dict

    def start(self, doc):
        # Ask the experiment for the desired analysis
        pipeline_name = doc["pipeline_name"]
        if pipeline_name not in self.pipelines:
            return None
        # if novel make new pipeline, else use current pipeline
        # Note that this doesn't support multiple starts (which requires
        pipeline = self.pipelines[pipeline_name].get(
            "pipeline", self.pipelines[pipeline_name]["func"]()
        )
        # modify the cache
        self.pipelines[pipeline_name]["cache_mod"](pipeline)
        # return the input
        return pipeline[self.pipelines[pipeline_name]["input"]]
