'''
config loader class
'''
class Configs:
    '''
    Initialize config
    '''
    def __init__(self):
        pass

    def db_lst(self, conf: dict):
        '''
        Initialize db config
        '''
        db_read_user = conf['db_read_user']
        db_read_pass = conf['db_read_pass']
        db_read_host = conf['db_read_host']
        db_read_name = conf['db_read_name']
        db_read_conn_string = f'mysql+pymysql://{db_read_user}:{db_read_pass}'
        db_read_conn_string += f'@{db_read_host}/{db_read_name}'

        db_write_user = conf['db_write_user']
        db_write_pass = conf['db_write_pass']
        db_write_host = conf['db_write_host']
        db_write_name = conf['db_write_name']
        db_write_conn_string = f'mysql+pymysql://{db_write_user}:{db_write_pass}'
        db_write_conn_string += f'@{db_write_host}/{db_write_name}'

        db_read_table = conf['db_read_tbl_name']

        db_conf_list = {
            'read_conn_string': db_read_conn_string,
            'write_conn_string': db_write_conn_string,
            'read_table': db_read_table
        }
        return db_conf_list

    def rules_transform_lst(self, conf: dict):
        '''
        Initialize transform config
        '''
        separator_ids = conf['separator_ids']
        column_ids = conf['column_ids']
        separator_names = conf['separator_names']
        column_names = conf['column_names']
        separator_categories = conf['separator_categories']
        column_categories = conf['column_categories']
        separator_attributes = conf['separator_attributes']
        column_attributes = conf['column_attributes']
        separator_tags = conf['separator_tags']
        column_tags = conf['column_tags']

        transform_conf_list = {
            'separator_ids': separator_ids,
            'column_ids': column_ids,
            'separator_names': separator_names,
            'column_names': column_names,
            'separator_categories': separator_categories,
            'column_categories': column_categories,
            'separator_attributes': separator_attributes,
            'column_attributes': column_attributes,
            'separator_tags': separator_tags,
            'column_tags': column_tags
        }
        return transform_conf_list
