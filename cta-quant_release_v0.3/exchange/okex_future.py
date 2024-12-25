from exchange_adaptor import ExchangeAdaptor


class OkexAdaptor(ExchangeAdaptor):

    def __init__(self, config_manager) -> None:
        super().__init__(config_manager)

    def _init_task(self):
        super()._init_task()
