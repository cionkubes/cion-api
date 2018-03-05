import re

import rethinkdb as r
from luqum.parser import parser
from luqum.tree import FieldGroup, Term, AndOperation, OrOperation, SearchField
import datetime


def match(value, expr):
    to_compare = expr.value.strip('"\'')
    # if to_compare[0] == ">":
    #     return r.epoch_time(value) > parse_date_time(to_compare[1:])
    # elif to_compare[0] == "<":
    #     return r.epoch_time(value) < parse_date_time(to_compare[1:])
    # elif to_compare.find('-') > -1:
    #     time_from, time_to = to_compare.split(' - ')
    #     return r.epoch_time(value).during(parse_date_time(time_from),
    #                                       parse_date_time(time_to))
    print(to_compare, value)
    return value.match(to_compare)


def parse_date_time(val):  # FIXME: this is a badly implemented bloated feature
    s = val.split('-')
    year = 1970
    month = r.january
    day = 0
    time = 0

    l = len(s)
    if l > 0:
        time = s[0]
    if l > 1:
        day = int(s[1])
    if l > 2:
        month = int(s[2])
    if l > 3:
        year = int(s[3])

    now = datetime.datetime.now()  # TODO: get timezone from browser
    if not year:
        year = now.year
    if not month:
        month = now.month
    if not day:
        day = now.day
    t = time.split('.')
    hour = int(t[0])
    minute = 0
    second = 0

    l = len(t)
    if l > 1:
        minute = int(t[1])
    if l > 2:
        second = int(t[2])
    return r.time(year, month, day, hour, minute, second, "Z")


def traverse(row, group, name):
    if isinstance(group, Term):
        return match(row[name], group)
    elif isinstance(group, SearchField):
        return traverse(row, group.expr, group.name)
    elif isinstance(group, FieldGroup):
        return traverse(row, group.children[0], name)
    elif isinstance(group, AndOperation):
        return all(traverse(row, operand, name) for operand in group.children)
    elif isinstance(group, OrOperation):
        return any(traverse(row, operand, name) for operand in group.children)


def get_filter(search_string):
    if not search_string.strip():
        print('Search string empty. Skipping filtering')
        return None

    tree = parser.parse(search_string)

    print(tree.__dict__)
    print(dir(tree))

    def filter_func(row):
        return traverse(row, tree, None)

    return filter_func


if __name__ == '__main__':
    filter_string = "event:(asdf) AND status:done"
    # filter_string = "event:(new-image)"
    # filter_string = "event:new-image"
    # filter_string = "event:new-image AND status:done OR
    # event:\"service-update\" AND status:erroneous"
    # filter_string = "event:new-image AND status:done OR event:update-service"
    # filter_string = "event:new-image AND status:done OR event:update-service"

    # parse("event:(new-image OR \"update service plz\") AND status:done")
    # parse("event:new-image")
    # parse("event:new-image AND status:done OR event:update-service")

    rows = [
        {"event": "new-image", "id": "50a06d20-d770-4b38-bff2-51f705f0552c",
         "image-name": "cion/web:rc-12", "status": "done",
         "time": 1519227573.857},
        {"event": "service-update",
         "id": "5a459948-7007-4f1d-9831-1911e5004c7c",
         "image-name": "cion/api:rc-3", "status": "erroneous",
         "time": 1519227585.747},
        {"event": "new-image", "id": "424145a4-e98f-4f52-be6a-e4d8ff4f0ef2",
         "image-name": "cion/api:rc-3", "status": "done",
         "time": 1519227590.036},
        {"event": "new-image", "id": "0b242126-0393-48dd-b365-c0f9b17982bb",
         "image-name": "cion/web:latest", "status": "done",
         "time": 1519227373.309},
        {"event": "new-image", "id": "1f19519b-f75b-4e06-9df2-5362a48ca2be",
         "image-name": "cion/api:rc-3", "status": "done",
         "time": 1519227591.014},
        {"event": "new-image", "id": "79b6cdff-58db-42bc-841f-137ccc20b80b",
         "image-name": "cion/api:rc-3", "status": "done",
         "time": 1519227587.826},
        {"event": "new-image", "id": "7f64d89f-dd64-4b0d-8326-a224f2063396",
         "image-name": "cion/web:rc-1", "status": "done",
         "time": 1519227571.194},
        {"event": "new-image", "id": "1288dadd-ffc1-4437-94a6-c3021cf39eb7",
         "image-name": "cion/web:rc-123", "status": "erroneous",
         "time": 1519227576.281},
        {"event": "service-update",
         "id": "a9c9762b-85ba-479a-8155-3fa5215d8302",
         "image-name": "cion/web:rc-2", "status": "done",
         "time": 1519227578.236},
        {"event": "service-update",
         "id": "45b2957e-d7aa-41de-a514-25f34e2e44e2",
         "image-name": "cion/api:rc-3", "status": "erroneous",
         "time": 1519227592.642},
        {"event": "new-image", "id": "ff0f20d0-e633-42d5-914d-e2ae6a99a763",
         "image-name": "cion/web:latest", "status": "done",
         "time": 1519227369.949},
        {"event": "service-update",
         "id": "9644431d-0e5c-444a-8933-019c66df3d21",
         "image-name": "cion/web:latest", "status": "erroneous",
         "time": 1519227366.208},
        {"event": "new-image", "id": "72484c18-ba75-4870-944a-ff9a5846abf3",
         "image-name": "cion/web:latest", "status": "done",
         "time": 1519227370.985},
        {"event": "service-update",
         "id": "9dea611b-b6a8-45c8-ae70-e58ad52acd1c",
         "image-name": "cion/web:rc-3", "status": "done",
         "time": 1519227581.237},
        {"event": "new-image", "id": "c3729f27-c5a9-45be-a952-f1d6e04f4477",
         "image-name": "cion/web:latest", "status": "done",
         "time": 1519227372.306},
        {"event": "new-image", "id": "ff498e5e-29b1-4031-ab56-3ffb99d598ee",
         "image-name": "cion/api:rc-3", "status": "erroneous",
         "time": 1519227588.956}]

    filter_func = get_filter(filter_string)
    for row in rows:
        if filter_func(row):
            print(row)
