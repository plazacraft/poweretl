import importlib
import poweretl.defs.model as p

#importlib.reload(p) 

class ConfigType2:
    
    def v(self):
        print ("ble")
    i = 0


z = p.config.Model()
z.tables = {}
z.tables['a'] = p.config.Table(id='1', name='table_1', columns={ 'a': p.config.Column(id='1', name='col_1', type="str") })

print (z)
#z.v()