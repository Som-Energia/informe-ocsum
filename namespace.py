#!/usr/bin/python3

class namespace(dict) :
	"""A dictionary that whose values can be accessed as attributes
	and can be loaded and dumped as YAML."""

	def __init__(self, *args, **kwd) :
		super(namespace, self).__init__(*args, **kwd)
		self.__dict__ = self

	def deepcopy(self) :
		return self.loads(self.dump())

	@classmethod
	def loads(cls, yamlContent) :
		import io
		return cls.load(io.StringIO(yamlContent))

	@classmethod
	def load(cls, filename) :

		def wrap(data) :
			if type(data) is dict :
				return namespace([
					(k, wrap(v))
					for k,v in data.items()
					])
			if isinstance(data, list) or isinstance(data, tuple) :
				return [wrap(v) for v in data]
			return data

		import yaml
		if hasattr(filename, 'read') :
			result = yaml.load(stream=filename)
		else :
			with open(filename) as f:
				result = yaml.load(stream=f)
		return wrap(result)

	def dump(self, filename=None) :

		def unwrap(data) :
			if type(data) is namespace :
				return dict([
					(k, unwrap(v))
					for k,v in data.items()
					])
			if isinstance(data, list) or isinstance(data, tuple) :
				return [unwrap(v) for v in data]
			return data

		import yaml
		if hasattr(filename,'write') :
			yaml.dump(unwrap(self), stream=filename,
				default_flow_style=False,
				allow_unicode=True,
				)
			return

		# TODO: Test None
		# TODO: Test file
		if filename is None or hasattr(filename,'write') :
			return yaml.dump(unwrap(self), stream=filename,
				default_flow_style=False,
				allow_unicode=True,
				)
		from codecs import open # Python2 compatibility
		with open(filename, 'w', encoding='utf-8') as f :
			yaml.dump(unwrap(self), stream=f,
				default_flow_style=False,
				allow_unicode=True,
				)

