import re

_named_param_re = re.compile(r'(?<!:):(\w+)')

class NamedQuery:
	__slots__ = ('sql', 'params')

	def __init__(self, sql):
		params = []
		def replace(m):
			params.append(m.group(1))
			return f'${len(params)}'
		self.sql = _named_param_re.sub(replace, sql)
		self.params = params

	def args(self, kwargs):
		return [kwargs[p] for p in self.params]

def named(sql):
	return NamedQuery(sql)
