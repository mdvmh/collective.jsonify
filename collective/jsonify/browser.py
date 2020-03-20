from Acquisition import aq_inner
from Products.Five.browser import BrowserView
from Products.CMFCore.utils import getToolByName
from collective.jsonify.wrapper import Wrapper

try:
    from hashlib import md5
except ImportError:
    from md5 import md5

try:
    import simplejson as json
except:
    import json


class JsonifyView(BrowserView):
    """Parameters:

    ACTIONS: QUERY -> use portal_catalog for data retrieving
             LIST -> use portal_catalog but return COMPACT list of live objects
             GET -> return the actual JSON of the objects (really needed?)
             PUT -> add object
             PATCH -> update object (just modified fields will be passed)
             DELETE -> delete object from portal
    """

    def __call__(self):
        self.params = self.request.form
        self.send_bin = self.params.get('send_bin', False)
        self.absolute_urls = self.params.get('absolute_urls', True)
        self.available = 'available' in self.params
        if not('action' in self.params):
            return
        if (self.params['action'] == 'query'):
            objs = self.action_query()
            # Do not return any object, just check for it
            if self.available:
                if (objs):
                    return len(objs)
                else:
                    return
            else:
                return self.get_it_out(objs)
        if (self.params['action'] == 'list'):
            raw_objs = self.action_query()
            return self.action_list(raw_objs)

    def action_list(self, raw_objs):
        objs = [
            {"uid": raw_obj.UID(),
             "path": "/".join(raw_obj.getPhysicalPath())}
            for raw_obj in raw_objs
        ]
        return self.push_json(objs)

    def action_query(self):
        #import pdb;pdb.set_trace()
        return []
        context = aq_inner(self.context)
        catalog = getToolByName(context, 'portal_catalog')
        if self.params:
            query = self.params
            query.pop('action', None)
            query.pop('send_bin', None)
            query.pop('absolute_urls', None)
            query.pop('available', None)
            query['path'] = '/'.join(self.context.getPhysicalPath())

        brains = catalog.searchResults(query)
        return [brain.getObject() for brain in brains if brain]

    def url_replacer(self, obj, searchstring, lookfor):
        """ this is to replace JUST relative urls with absolute urls
        """
        context = aq_inner(self.context)
        root = getToolByName(context, 'portal_url').getPortalObject()
        position = 0
        for found in range(searchstring.count(lookfor)):
            position = searchstring.index(lookfor, position)
            poslookfor = position + len(lookfor)
            if searchstring[poslookfor:poslookfor + 4] != 'http':
                if searchstring[poslookfor:poslookfor + 1] == '/':
                    # it's a relative url to the root - use root.absolute_url()
                    url_to_add = root.absolute_url()
                else:
                    # it's a relative url to the actual object position
                    url_to_add = obj.aq_parent.aq_inner.absolute_url() + '/'
                searchstring = searchstring[:position] +\
                    lookfor + url_to_add + searchstring[poslookfor:]
                position = poslookfor + 4
            else:
                position = poslookfor
        return searchstring

    def get_it_out(self, raws):
        objs = []
        for raw in raws:
            wrapped = Wrapper(raw)
            for key in wrapped.keys():
                if key.startswith('_datafield_'):
                    # get HASH: useful to check changes with APP side before
                    # download it
                    m = md5()
                    m.update(wrapped[key]['data'])
                    wrapped[key]['md5'] = m.hexdigest()
                    if not(self.send_bin):
                        wrapped[key]['data'] = ''
                else:
                    if self.absolute_urls and self.absolute_urls != 'False':
                        if type(wrapped[key]) in (unicode, str):
                            for tosearch in ['src=\"', 'href=\"']:
                                wrapped[key] = self.url_replacer(
                                    raw,
                                    wrapped[key],
                                    tosearch
                                )

            objs.append(wrapped)
        return self.push_json(objs)

    def push_json(self, objs):
        mongodb_ids = []
        for obj in objs:

            ### TODO
            # attachments, limit mongodb? 16 mb
            # versende-tasks, versende-status
            # uebernahme message-ids, seq-ids
            # vokabularien
            # registries plone control panel

            try:

                ### adjust
                adjusted_data = self.adjust(obj)

                ### use custom EMS JSON Encoder to make data available for json dumping
                json_data = json.dumps(adjusted_data, cls=EMSEncoder)
                result = self.save(json.loads(json_data))
                mongodb_ids.append(result)
                #JSON = json.dumps(objs, cls=EMSEncoder)
                #self.request.response.setHeader("Content-type", "application/json")
                #return JSON
            except Exception, e:
                #import pdb;pdb.set_trace()
                print str(e)
                pass
                #return 'ERROR: wrapped object is not serializable: %s' % str(e)

        ### TMP user, usergroups
        ### usergroups
        import plone.api
        from pymongo import MongoClient

        client = MongoClient(host='mongodb2', port=27017)
        db = client.migration

        groups = plone.api.group.get_groups()
        for group in groups:
            data = dict(
                group_id = group.getGroupId(),
                member_ids = group.getAllGroupMemberIds(),
                roles = group.getRoles(),
                properties = group.getProperties()
            )
            db.usergroups.save(data)

        ### user
        passwords = self.context.acl_users.source_users._user_passwords
        users = plone.api.user.get_users()
        for user in users:
            data = dict(
                user_id = user.getUserName(),
                password = passwords.get(user.getUserName()),
                # TODO: getRoles returns roles of usergroups of the user too,
                # how the get roles assigned to the user?
                roles = user.getRoles(),
                fullname = user.fullname,
                description = user.description,
                location = user.location,
                email = user.email,
                homepage = user.home_page,
            )
            db.users.save(data)

        return mongodb_ids

    def save(self, obj):
        from pymongo import MongoClient
        results = []

        events_cts = [
            'Incident',
            'Activity_Unplanned',
            'Activity_AddService',
            'Activity_Freetext',
            'Allocator',
            'Message',
            'ActivityMessage',
            'Recommendation',
            'FileAttachment',
            'ImageAttachment'
        ]

        vocabs_cts = [
            'AliasVocabulary',
            'SimpleVocabulary',
            'SimpleVocabularyTerm',
            'SortedSimpleVocabulary',
            'TreeVocabulary',
            'TreeVocabularyTerm',
            'VdexFileVocabular',
            'VocabularyLibrary'
        ]

        client = MongoClient(host='mongodb2', port=27017)
        db = client.migration

        # events
        if '_type' in obj.keys() and obj['_type'] in events_cts:
            result = db.events.save(obj)
        # vocabs
        elif '_type' in obj.keys() and obj['_type'] in vocabs_cts:
            result = db.vocabs.save(obj)
        # cms
        else:
            result = db.cms.save(obj)

        results.append(result)
        return results

    def adjust(self, obj):

        ### Templates: fields: firstmsg, mainmsg, endmsg
        ### process data, because type is not dict, but ZPublisher.HTTPRequest.record
        ### NOTE/TODO?: JSONEncoder gives us TypeError after copying record data and casting to dict, needs deeper investigation
        ### workaround: we prepare data before passing JSONEncoder for now ...
        fields = ['firstmsg', 'mainmsg', 'endmsg']
        for field in fields:
            if field in obj and isinstance(obj[field], list):
                new_items = []
                for item in obj[field]:
                    new_item = dict(item.copy())
                    new_items.append(new_item)
                obj[field] = new_items

        ### key '_id' provides plone id (unique in folder only, but not unique in Plone), but mongoDB makes use of '_id' to provide unique id, so we
        ### a) store '_id' as '_plone_id'
        ### b) copy '_uid' (unique Plone id) to '_id' to use Plone UID as unique id in mongoDB
        obj['_plone_id'] = obj['_id']
        obj['_id'] = obj['_uid']
        
        return obj


### MH: json encode for EMS specific data
from datetime import date, datetime
import json
from ZPublisher.HTTPRequest import record

class EMSEncoder(json.JSONEncoder):
    def default(self, o):
        # datetime, date
        if isinstance(o, (datetime, date)):
            return o.isoformat()

        # HTTPRequest.record: e.g. firstmsg, mainmsg, endmsg in templates:
        # cast to dict
        if isinstance(o, record):
            return json.JSONEncoder.default(self, dict(o.copy()))

        return json.JSONEncoder.default(self, o)
