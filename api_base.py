from logging import WARNING

from flask import request, jsonify, current_app as app
from sqlalchemy.exc import OperationalError
from flask_restful import abort

from ndc_mapping.database import Database
import ndc_mapping.models as models


class Api_Base(object):
    record_count = 0

    def __init__(self, table_class, request):
        self.table_class = table_class
        self.request = request
        db = Database()
        self.Session = db.get_session()

    def _check_access(self, access_lvl):
        return True

    def _search(self, table):
        DBSession = self.Session()
        req = self.request.args
        sql = DBSession.query(table)

        embed_keys_list = req.get('embed', '').split(',')
        for key in embed_keys_list:
            if "_embed_{0}".format(key) in dir(self):
                sql = getattr(self, "_embed_{0}".format(key))(sql)

        exclude_keys = [
            'api_key',
            'embed',
            'sort',
            'start_index',
            'page_size',
            'filter']
        search_keys = set([k for k in req.keys() if k not in exclude_keys])

        for key in search_keys:
            search_list = req.getlist(key)

            if hasattr(table, key):
                if len(search_list) > 1:
                    sql = sql.filter(getattr(table, key).in_(search_list))
                elif len(search_list) == 1:
                    sql = sql.filter(getattr(table, key).like(search_list[0]))
                else:
                    continue
            else:
                sql = sql.filter('{0} like "{1}"'.format(key, search_list))

        filters_array = req.getlist('filter')
        for filter in filters_array:
            sql = sql.filter(filter)

        sort_array = self._sort(sort_name="sort")
        for sort in sort_array:
            sql = sql.order_by(sort)

        # TODO: Need to fix this before we can do embedding.
        # sql = sql.group_by(getattr(table, inspect(table).primary_key[0].name))
        try:
            self.record_count = sql.count()
        except OperationalError:
            raise ApiQueryError('Your parameters are not formed properly.')

        if req.get('start_index') and req.get('page_size'):
            sql = sql.limit(req.get('page_size')).offset(req.get('start_index'))

        try:
            return sql.all()
        except OperationalError:
            raise ApiQueryError('Your parameters are not formed properly.')

    def _sort(self, sort_name="sort"):
        query_sort_array = self.request.args.get(sort_name, '').split(',')
        sort = []
        for key in query_sort_array:
            sort_by = key[1:] if key[:1] in ('-', '+', ' ') else key
            sort_array = sort_by.split('.')

            if len(sort_array) == 1:
                if hasattr(self.table_class, sort_by):
                    sort_obj = getattr(self.table_class, sort_by)
                else:
                    continue
            elif len(sort_array) == 2:
                class_obj = getattr(models, sort_array[0].title())
                sort_obj = getattr(class_obj, sort_array[1])
            else:
                continue

            if key[:1] in ('+', ' '):
                sort.append(sort_obj.asc())
            else:
                sort.append(sort_obj.desc())

        return sort

    def collection_get(self):
        if not self._check_access(self.get_access):
            abort(403)

        results = self._search(self.table_class)

        if results:
            if hasattr(results[0], '_fields'):
                return_list = [dict(
                    # r[0] - First item in the result tuple which should
                    #   be the original object searched.
                    r[0].to_dict().items() + {
                        l.lower(): getattr(r, l).to_dict() if getattr(r, l) else {}
                        for l in r._fields[1:]
                    }.items()
                ) for r in results]
            else:
                return_list = [r.to_dict() for r in results]
        else:
            return_list = []

        return jsonify({"data": return_list, "total_record_count": self.record_count})

    def collection_post(self):
        DBSession = self.Session()
        if not self._check_access(self.post_access):
            abort(403)

        post = self.request.form or self.request.json_body
        attrs = {k: v for k, v in post.iteritems() if k in self.table_class.__table__.columns}
        result = self.table_class(**attrs)
        DBSession.add(result)
        DBSession.commit()
        DBSession.add(result)

        return jsonify({"data": result.to_dict()})

    def single_get(self, table_id):
        DBSession = self.Session()
        if not self._check_access(self.get_access):
            abort(403)

        result = DBSession.query(self.table_class).get(table_id)

        if result:
            return jsonify({"data": result.to_dict()})
        abort(404)

    def single_delete(self, table_id):
        DBSession = self.Session()
        if not self._check_access(self.delete_access):
            abort(403)

        result = DBSession.query(self.table_class).get(table_id)
        DBSession.delete(result)
        DBSession.commit()
        return jsonify({"data": {"message": "The resource has been deleted."}})

    def single_put(self, table_id):
        DBSession = self.Session()
        if not self._check_access(self.put_access):
            abort(403)

        result = DBSession.query(self.table_class).get(table_id)

        if not result:
            abort(404)

        post = self.request.form or self.request.json_body
        for field, value in post.iteritems():
            if hasattr(result, field):
                setattr(result, field, value)

        DBSession.add(result)
        DBSession.commit()
        return jsonify({"data": {"message": "The resource has been updated."}})

class ApiQueryError(Exception):
    pass
