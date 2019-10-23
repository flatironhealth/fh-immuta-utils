class Paginator(object):
    """
    Handle immuta client pagination.
    assumptions:
     - The result of the client action returns an object with the list of objects in the key 'hits'
     - The client actions have the parameters `size` and `offset`

    example:
    >>> client = ImmutaClient(**client_config)
    >>> with Paginator(client.get_data_source_list, search_text='test') as paginator:
    ...    for data_source in paginator:
    ...        print(data_source['id'])

    """

    def __init__(self, action, *args, **kwargs):
        self.action = action
        self.action_args = args
        self.action_kwargs = kwargs
        self._size = self.action_kwargs.pop("size", 50)
        self._offset = self.action_kwargs.pop("offset", 0)
        self.has_next_page = True

    def __iter__(self):
        while self.has_next_page:
            result = self.next_page()
            for hit in result["hits"]:
                yield hit

    def next_page(self):
        result = self._execute_action()
        self._offset += self._size
        if len(result["hits"]) < self._size:
            self.has_next_page = False
        return result

    def current_page(self):
        return self._offset / self._size

    def _execute_action(self):
        return self.action(
            *self.action_args,
            **self.action_kwargs,
            size=self._size,
            offset=self._offset
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._offset = 0
        self.has_next_page = False
