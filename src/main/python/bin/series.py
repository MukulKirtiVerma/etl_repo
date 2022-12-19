'''
Transform and generate series class
'''
import pandas as pd


class DataSeries():
    '''
    Initialized class DataSeries
    '''
    def __init__(self, dict_series_config):
        self.extract_row = dict_series_config['dict_row']
        self.rules_config = dict_series_config['rules_config']

    def remove_star_prefix(self, val: str):
        '''
        :param val: type str
        :return: rval: type str
        '''
        rval = val
        if rval[:1] == '*':
            rval = rval[1:]

        return rval

    def get_platform_value(self, val: str):
        '''
        :param val: type str
        :return: pfm: type, rval: type str
        '''
        pfm = ''
        rval = val
        if val[:1] == '$':
            pfm = val[1:4]
            rval = val[4:]

        return pfm, rval

    def geoips(self, geoip):
        '''
        func transform geoip
        '''
        sessid = self.extract_row['sessid']
        tsm = self.extract_row['tsupdate']
        merchant = self.extract_row['merchant']
        ctrcode = 'ID'
        ctrname = 'Indonesia'
        regname = 'Unknown'
        ctyname = 'Unknown'
        geoasn = 'Unknown'
        uip = 'Unknown'

        arr_geoip = geoip.split(',')
        transform_geoip = len(arr_geoip)
        if len(geoip) > 1:
            ctrcode = arr_geoip[0]
            ctrname = arr_geoip[1]
            if not arr_geoip[2].isascii():
                regname = 'Unknown'
            else:
                regname = arr_geoip[2]
            if not geoip[3].isascii():
                ctyname = 'Unknown'
            else:
                ctyname = arr_geoip[3]

            geoasn = arr_geoip[4]
            if geoip[5] == '':
                uip = 'Unknown'

        geoip_series = [[sessid, tsm, tsm, merchant,
                ctrcode, ctrname, regname,
                ctyname, geoasn, uip]]
        vm_geoip = pd.DataFrame(geoip_series,
            columns = ['sessid', 'tscreated', 'tsupdated', 'merchant',
                'ctrcode', 'ctrname', 'regname',
                'ctyname', 'geoasn','uip'])
        query = f"UPDATE video_measure SET tsupdated={tsm}"
        query += f", ctrcode='{ctrcode}', ctrname='{ctrname}'"
        query += f", regname='{regname}'"
        query += f", ctyname='{ctyname}', geoasn='{geoasn}'"
        query += f", uip='{uip}' WHERE sessid='{sessid}'"
        return vm_geoip, query, transform_geoip

    def userids(self, userid):
        '''
        func transform userid
        '''
        sessid = self.extract_row['sessid']
        tsm = self.extract_row['tsupdate']
        merchant = self.extract_row['merchant']
        userid_series = [[sessid, tsm, tsm, merchant, userid]]
        vm_userid = pd.DataFrame(userid_series,
            columns = ['sessid', 'tscreated', 'tsupdated',
                'merchant', 'userid'])
        query = f"UPDATE video_measure SET tsupdated={tsm}"
        query += f", userid='{userid}' WHERE sessid='{sessid}'"
        return vm_userid, query

    def duuid1(self, duuid):
        '''
        func transform duuid for video measure
        '''
        sessid = self.extract_row['sessid']
        tsm = self.extract_row['tsupdate']
        merchant = self.extract_row['merchant']
        duuid_series = [[sessid, tsm, tsm, merchant, duuid]]
        df_unload = pd.DataFrame(duuid_series,
            columns = ['sessid', 'tscreated', 'tsupdated', 'merchant', 'duuid'])
        query = f"UPDATE video_measure SET tsupdated={tsm}"
        query += f", duuid='{duuid}' WHERE sessid='{sessid}'"
        return df_unload, query

    def duuid2(self, duuid):
        '''
        func transform duuid fron vm_duuid
        '''
        sessid = self.extract_row['sessid']
        tsm = self.extract_row['tsupdate']
        vm_duuid_series = [[sessid, duuid, tsm]]
        vm_duuid = pd.DataFrame(vm_duuid_series,
                columns = ['sessid', 'duuid', 'nts'])
        return vm_duuid

    def uadevs(self, uadev):
        '''
        func transform uadev
        '''
        sessid = self.extract_row['sessid']
        tsm = self.extract_row['tsupdate']
        merchant = self.extract_row['merchant']
        devops = ''
        devver = ''
        devmdl = ''
        devbrd = ''
        devmfr = ''
        arr_uadev = uadev.split(",")
        transform_uadev = len(arr_uadev)
        if len(arr_uadev) > 1:
            devops = arr_uadev[0]
            devver = arr_uadev[1]
            devmdl = arr_uadev[2]
            devbrd = arr_uadev[3]
            devmfr = arr_uadev[3]

        devops_series = [[sessid, tsm, tsm, merchant,
            devops, devver, devmdl,
            devbrd, devmfr]]
        vm_uadev = pd.DataFrame(devops_series,
            columns = ['sessid', 'tscreated', 'tsupdated', 'merchant',
                'devops', 'devver', 'devmdl', 'devbrd', 'devmfr'])

        query = f"UPDATE video_measure SET tsupdated={tsm}"
        query += f", devops='{devops}', devver='{devver}'"
        query += f", devmdl='{devmdl}', devbrd='{devbrd}'"
        query += f", devmfr='{devmfr}' WHERE sessid='{sessid}'"
        return vm_uadev, query, transform_uadev

    def ids(self, rowid):
        '''
        func transform id
        '''
        sessid = self.extract_row['sessid']
        tsm = self.extract_row['tsupdate']
        id_series = [[sessid, tsm, tsm, self.extract_row['merchant']]]
        column_ids = ['sessid', 'tscreated', 'tsupdated', 'merchant']
        query = f'UPDATE video_measure SET tsupdated={tsm}, '

        transform_id = 0
        rules_separator_ids = self.rules_config['separator_ids']
        rules_column_ids = self.rules_config['column_ids']
        for _, series in enumerate(id_series):
            for _, sep_ids in enumerate(rules_separator_ids):
                aid = rowid.split(sep_ids, len(rules_column_ids))
                transform_id = len(aid)

                for i, j in enumerate(aid):
                    series.append(j)
                    column_ids.append(f"id{i+1}")

                    if (i+1) == len(aid):
                        query += f'id{i+1}={j}'
                    else:
                        query += f'id{i+1}={j}, '

        vm_id = pd.DataFrame(id_series, columns=column_ids)
        query += f" WHERE sessid='{sessid}'"
        return vm_id, query, transform_id

    def names(self, name):
        '''
        func transform name
        '''
        sessid = self.extract_row['sessid']
        tsm = self.extract_row['tsupdate']
        name_series = [[sessid, tsm, tsm, self.extract_row['merchant']]]
        column_names = ['sessid', 'tscreated', 'tsupdated', 'merchant']
        query = f'UPDATE video_measure SET tsupdated={tsm}, '

        transform_name = 0
        rules_separator_names = self.rules_config['separator_names']
        for _, series in enumerate(name_series):
            for _, rule_sep_name in enumerate(self.rules_config['separator_names']):
                aname = None
                if isinstance(name, int):
                    sname = str(name)
                    aname = sname.split(rules_separator_names[0], len(rules_separator_names))
                    transform_name = len(aname)
                else:
                    aname = name.split(rule_sep_name, len(rules_separator_names))
                    transform_name = len(aname)

                for index, value in enumerate(aname):
                    series.append(value)
                    column_names.append(f'name{index+1}')

                    if (index+1) == len(aname):
                        query += f"name{index+1}='{value}'"
                    else:
                        query += f"name{index+1}='{value}', "

        vm_name = pd.DataFrame(name_series, columns=column_names)
        query += f" WHERE sessid='{sessid}'"
        return vm_name, query, transform_name

    def categories(self, category):
        '''
        func transform category
        '''
        sessid = self.extract_row['sessid']
        tsm = self.extract_row['tsupdate']
        merchant = self.extract_row['merchant']
        category_series = [[sessid, tsm, tsm, merchant]]
        column_category = ['sessid', 'tscreated', 'tsupdated', 'merchant']
        query = f'UPDATE video_measure SET tsupdated={tsm}, '

        transform_category = 0
        rules_separator_categories = self.rules_config['separator_categories']
        for _, series in enumerate(category_series):
            for _, rule_sep_category in enumerate(rules_separator_categories):
                categories = category.split(rule_sep_category, len(rules_separator_categories))
                transform_category = len(category)
                for index, value in enumerate(categories):
                    series.append(value)
                    column_category.append(f'category{index+1}')

                    if (index+1) == len(categories):
                        query += f'category{index+1}={value}'
                    else:
                        query += f'category{index+1}={value}, '
        vm_category = pd.DataFrame(category_series, columns=column_category)
        query += f" WHERE sessid='{sessid}'"
        return vm_category, query, transform_category

    def tags(self, tag):
        '''
        func transform tags
        '''
        sessid = self.extract_row['sessid']
        tsm = self.extract_row['tsupdate']
        merchant = self.extract_row['merchant']
        tags_series = [[sessid, tsm, tsm, merchant]]
        column_tags = ['sessid', 'tscreated', 'tsupdated', 'merchant']
        query = f'UPDATE video_measure SET tsupdated={tsm}, '

        transform_tags = 0
        rules_separator_tags = self.rules_config['separator_tags']
        for _, series in enumerate(tags_series):
            for _, rule_separator_tag in enumerate(rules_separator_tags):
                tags = tag.split(rule_separator_tag, len(rules_separator_tags))
                transform_tags = len(tags)
                for index, value in enumerate(tags):
                    series.append(value)
                    column_tags.append(f'tag{index+1}')

                    if (index+1) == len(tags):
                        query += f"tag{index+1}='{value.strip()}'"
                    else:
                        query += f"tag{index+1}='{value.strip()}', "
        vm_tag = pd.DataFrame(tags_series, columns=column_tags)
        query += f" WHERE sessid='{sessid}'"
        return vm_tag, query, transform_tags

    def attributes(self, attribute):
        '''
        func transform attributes
        '''
        sessid = self.extract_row['sessid']
        tsm = self.extract_row['tsupdate']
        merchant = self.extract_row['merchant']
        attr_series = [[sessid, tsm, tsm, merchant]]
        column_attr = ['sessid', 'tscreated', 'tsupdated', 'merchant']
        query = f'UPDATE video_measure SET tsupdated={tsm}, '

        tranform_attr = 0
        rules_separator_attributes = self.rules_config['separator_attributes']
        for _, series in enumerate(attr_series):
            for _, rule_separator_attribute in enumerate(rules_separator_attributes):
                attr = attribute.split(rule_separator_attribute)
                tranform_attr = len(attr)
                for index, value in enumerate(attr):
                    series.append(value)

                    if (index+1) == len(attr):
                        column_attr.append("hardware_type")
                        query += f"hardware_type='{value.strip()}'"
                    else:
                        column_attr.append(f'attribute{index+1}')
                        query += f"attribute{index+1}='{value.strip()}', "

        vm_attr = pd.DataFrame(attr_series, columns=column_attr)
        query += f" WHERE sessid='{sessid}'"
        return vm_attr, query, tranform_attr

    def load1(self, aload):
        '''
        func transform load for video measure
        '''
        sessid = self.extract_row['sessid']
        tsm = self.extract_row['tsupdate']
        merchant = self.extract_row['merchant']
        _, val_load = self.get_platform_value(aload)
        load_series = [[sessid, tsm, tsm, merchant, val_load]]
        df_load = pd.DataFrame(load_series,
            columns = ['sessid', 'tscreated',
                'tsupdated', 'merchant', 'nload'])

        query = f"UPDATE video_measure SET tsupdated={tsm}"
        query += f", nload={int(val_load)} WHERE sessid='{sessid}'"
        return df_load, query

    def load2(self, aload):
        '''
        func transform load for vm_load
        '''
        sessid = self.extract_row['sessid']
        pfm_load, val_load = self.get_platform_value(aload)
        vm_load_series = [[sessid, val_load, pfm_load]]
        vm_load = pd.DataFrame(vm_load_series, columns=['sessid', 'nts', 'pfm'])
        return vm_load

    def durations(self, span_time):
        '''
        func transform duration
        '''
        sessid = self.extract_row['sessid']
        tsm = self.extract_row['tsupdate']
        merchant = self.extract_row['merchant']
        rep_duration = str(span_time).replace(",", ".")
        duration_series = [[sessid, tsm, tsm, merchant, int(rep_duration)]]
        vm_duration = pd.DataFrame(duration_series,
            columns=['sessid', 'tscreated',
                'tsupdated', 'merchant', 'duration'])
        query = f"UPDATE video_measure SET tsupdated={tsm}"
        query += f", duration={int(rep_duration)} WHERE sessid='{sessid}'"
        return vm_duration, query

    def bitrates(self, bitrate):
        '''
        func transform bitrate
        '''
        sessid = self.extract_row['sessid']
        pfm_bitrate, val_bitrate = self.get_platform_value(bitrate)
        arr_bitrate = val_bitrate.split(",")

        if len(arr_bitrate) > 2:
            arr_bitrate[2] = int(arr_bitrate[2])

        vm_bitrate = None
        vm_table = None
        if not (arr_bitrate[0] == "0" or arr_bitrate[0] == "-1"):
            if pfm_bitrate is not None and len(arr_bitrate) > 2:
                arr_bitrate[0] = self.remove_star_prefix(arr_bitrate[0])
                abr_series = [[sessid, arr_bitrate[2], arr_bitrate[0], pfm_bitrate]]
                vm_bitrate = pd.DataFrame(abr_series, columns=['sessid', 'nts', 'nvalue', 'pfm'])
                vm_table = 'vm_abr'
        if not (arr_bitrate[1] == "0" or arr_bitrate[1] == "-1"):
            if pfm_bitrate is not None and len(arr_bitrate) > 2:
                arr_bitrate[1] = self.remove_star_prefix(arr_bitrate[1])
                vbr_series = [[sessid, arr_bitrate[2], arr_bitrate[1], pfm_bitrate]]
                vm_bitrate = pd.DataFrame(vbr_series, columns=['sessid', 'nts', 'nvalue', 'pfm'])
                vm_table = 'vm_vbr'
        return vm_bitrate, vm_table

    def plays(self, play):
        '''
        func transform play
        '''
        sessid = self.extract_row['sessid']
        pfm_play, val_play = self.get_platform_value(play)
        val_play = self.remove_star_prefix(val_play)

        play_series = [[sessid, val_play, pfm_play]]
        vm_play = pd.DataFrame(play_series, columns=['sessid', 'nts', 'pfm'])
        return vm_play

    def pauses(self, pause):
        '''
        func transform pause
        '''
        sessid = self.extract_row['sessid']
        pfm_pause, val_pause = self.get_platform_value(pause)
        val_pause = self.remove_star_prefix(val_pause)
        pause_series = [[sessid, val_pause, pfm_pause]]
        vm_pause = pd.DataFrame(pause_series, columns=['sessid', 'nts', 'pfm'])
        return vm_pause

    def seeks(self, seek):
        '''
        func transform seek
        '''
        sessid = self.extract_row['sessid']
        pfm_seek, val_seek = self.get_platform_value(seek)
        sseek = val_seek.replace(",", "^")
        aseek = sseek.split("^")
        aseek[0] = self.remove_star_prefix(aseek[0])
        seek_series = [[sessid, aseek[0], aseek[1], pfm_seek]]
        vm_seek = pd.DataFrame(seek_series, columns=['sessid', 'nts', 'nvalue', 'pfm'])
        return vm_seek

    def playings(self, playing):
        '''
        func transform playing
        '''
        sessid = self.extract_row['sessid']
        pfm_playing, val_playing = self.get_platform_value(playing)
        splaying = val_playing.replace(",", "^")
        aplaying = splaying.split("^")
        aplaying[0] = self.remove_star_prefix(aplaying[0])
        playing_series = [[sessid, aplaying[0], aplaying[1], pfm_playing]]
        vm_playing = pd.DataFrame(playing_series, columns=['sessid', 'nts', 'svalue', 'pfm'])
        return vm_playing

    def buffers(self, buffer):
        '''
        func transform buffer
        '''
        sessid = self.extract_row['sessid']
        pfm_buffer, val_buffer = self.get_platform_value(buffer)
        val_buffer = self.remove_star_prefix(val_buffer)
        buffer_series = [[sessid, val_buffer, pfm_buffer]]
        vm_buffer = pd.DataFrame(buffer_series, columns=['sessid', 'nts', 'pfm'])
        return vm_buffer

    def errors(self, error):
        '''
        func transform error
        '''
        sessid = self.extract_row['sessid']
        pfm_error, val_error = self.get_platform_value(error)
        serror = val_error.replace(",", "^")
        aerror = serror.split("^")

        if len(aerror) > 1:
            if len(aerror[1]) > 1024:
                aerror[1] = aerror[1][:1023]

        error_series = [[sessid, aerror[0], aerror[1], pfm_error]]
        vm_error = pd.DataFrame(error_series, columns=['sessid', 'nts', 'msg', 'pfm'])
        query = f"UPDATE vm_error SET nts={aerror[0]}"
        query += f", msg='{aerror[1]}' WHERE sessid='{sessid}'"
        return vm_error, query

    def completes(self, complete):
        '''
        func transform complete
        '''
        sessid = self.extract_row['sessid']
        pfm_complete, val_complete = self.get_platform_value(complete)

        complete_series = [[sessid, int(val_complete), pfm_complete]]
        vm_complete = pd.DataFrame(complete_series, columns=['sessid', 'nts', 'pfm'])
        return vm_complete

    def unload1(self, unload):
        '''
        func transform unload for video measure
        '''
        sessid = self.extract_row['sessid']
        tsm = self.extract_row['tsupdate']
        merchant = self.extract_row['merchant']
        _, val_unload = self.get_platform_value(unload)

        unload_series = [[sessid, tsm, tsm, merchant, unload]]
        df_unload = pd.DataFrame(unload_series,
            columns = ['sessid', 'tscreated',
                'tsupdated', 'merchant', 'unload'])

        query = f"UPDATE video_measure SET tsupdated={tsm}"
        query += f", nload={int(val_unload)} WHERE sessid='{sessid}'"
        return df_unload, query

    def unload2(self, unload):
        '''
        func transform unload for vm_unload
        '''
        sessid = self.extract_row['sessid']
        pfm_unload, val_unload = self.get_platform_value(unload)
        vm_unload_series = [[sessid, val_unload, pfm_unload]]
        vm_unload = pd.DataFrame(vm_unload_series, columns=['sessid', 'nts', 'pfm'])
        return vm_unload
