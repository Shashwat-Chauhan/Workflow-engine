from miniflow.core import Workflow, step
from miniflow.logger import get_logger

logger = get_logger("workflows.example")

class ExampleWorkflow(Workflow):
    @step(order=0)
    def fetch_data(self, ctx):
        logger.info("Fetching data...")
        # pretend fetching
        data = {"numbers": [1,2,3,4,5]}
        return data

    @step(order=1)
    def process(self, ctx):
        data = ctx.state.get("fetch_data")
        numbers = data["numbers"]
        total = sum(numbers)
        average = total / len(numbers)
        return {"total": total, "avg": average}

    @step(order=2, retries=2)
    def save(self, ctx):
        results = ctx.state.get("process")
        logger.info("Saving results: %s", results)
        # simple save to file or external sink; here just return ok
        return {"saved": True}