
from pprint import pformat
import json
import errno

class ModuleConfig:

    def _tree_spec_to_plain(self, prefix, options_spec):
        plain_spec = {}
        for key,val in options_spec.items():
            plain_key = prefix+'.'+key if prefix else key
            if isinstance(val, dict):
                section = self._tree_spec_to_plain(plain_key, val)
                for k,v in section.items():
                    plain_spec[k] = v
            else:
                plain_spec[plain_key] = val
        return plain_spec

    def __init__(self, module, options_spec):
        self._module = module
        self._spec = self._tree_spec_to_plain('', options_spec)
        self._cfg = {}
        for key,val in self._spec.items():
            self._cfg[key] = val[0](val[1])

    def _str_to_bool(self, val):
        bool_false = set(['0', '', 'false'])
        return not val.lower() in bool_false

    def _str_to_val(self, plain_key, val):
        if plain_key in self._spec:
            spec = self._spec[plain_key]
            if spec[0] is bool:
                return self._str_to_bool(val)
            else:
                return spec[0](val)

    def _bool_to_str(self, val):
        return '1' if val else '0'

    def _val_to_str(self, val):
        if isinstance(val, bool):
            return self._bool_to_str(val)
        else:
            return str(val)

    def _write(self, plain_key, value):
        if plain_key in self._spec:
            self._module.set_config(plain_key, self._val_to_str(value))

    def load(self):
        for key,spec in self._spec.items():
            value = self._module.get_config(key, None)
            if value is None:
                self._cfg[key] = spec[1]
            else:
                self._cfg[key] = spec[0](value)

        for key,value in self._cfg.items():
            self._module.log.info('cfg: %s = %s' % (key, pformat(value)))

    def init(self):
        self._module.log.info('Initializing config options')

        for key,value in self._spec.items():
            v = self._module.get_config(key, None)
            if v is None:
                self._write(key, value)

        for key,value in self._cfg.items():
            self._module.log.info('cfg: %s = %s' % (key, pformat(value)))

    def reset(self, plain_key):
        if plain_key in self._spec:
            spec = self._spec[plain_key]
            self._cfg[plain_key] = spec[1]
            self._write(plain_key, spec[1])

    def set_str(self, plain_key, str_value):
        if plain_key in self._spec:
            val = self._str_to_val(plain_key, str_value)
            self._cfg[plain_key] = val
            self._write(plain_key, val)

    def enable(self, plain_key):
        self._cfg[plain_key] = True
        self._write(plain_key, True)

    def disable(self, plain_key):
        self._cfg[plain_key] = False
        self._write(plain_key, False)

    def get(self, *key_path):
        key = '.'.join(key_path)
        return self._cfg.get(key, None)

    def get_section(self, *section_path):
        prefix = '.'.join(section_path)
        section = {}
        for k,v in self._cfg.items():
            if k.startswith(prefix):
                key = k[len(prefix)+1:]
                section[key] = v
        return section

    def dump(self):
        result = {}
        for key,val in self._cfg.items():
            path = key.split('.')
            ptr = result
            for k in path[:-1]:
                if k not in ptr:
                    ptr[k] = {}
                ptr = ptr[k]
            ptr[path[-1]] = val
        return result

    @staticmethod
    def get_commands(module_name):
        return [
            {
                "cmd": module_name+" cfg set name=key,type=CephString, name=value,type=CephString",
                "desc": "Set config value",
                "perm": "rw",
            },
            {
                "cmd": module_name+" cfg reset name=key,type=CephString",
                "desc": "Reset config-key option to default",
                "perm": "rw",
            },
            {
                "cmd": module_name+" cfg init",
                "desc": "Initialize config-key options",
                "perm": "rw",
            },
            {
                "cmd": module_name+" cfg dump",
                "desc": "Dump config-key options",
                "perm": "r",
            },
        ]

    def handle_command(self, module_name, command):
        if command['prefix'] == module_name+' cfg set':
            key = str(command['key'])
            value = str(command['value'])
            if key in self._spec:
                self.set_str(key, value)
                return (0, '', '')
            else:
                return (-errno.EINVAL, '', 'key "%s" not found in config' % key)

        elif command['prefix'] == module_name+' cfg reset':
            key = str(command['key'])
            if key in self._spec:
                self.reset(key)
                return (0, '', '')
            else:
                return (-errno.EINVAL, '', 'key "%s" not found in config' % key)

        elif command['prefix'] == module_name+' cfg init':
            self.init()
            return (0, '', '')

        elif command['prefix'] == module_name+' cfg dump':
            return (0, json.dumps(self.dump(), indent=2), '')

        else:
            return (-errno.EINVAL, '', "Command not found '{0}'".format(command['prefix']))
