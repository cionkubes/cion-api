class Permission:
    def __init__(self, check_fn):
        self.check_fn = check_fn

    def has_permission(self, *args, **kwargs):
        return self.check_fn(*args, **kwargs)

    def __and__(self, other):
        async def check(*args, **kwargs):
            return await self.has_permission(*args, **kwargs) \
                   and await other.has_permission(*args, **kwargs)

        return Permission(check)

    def __or__(self, other):
        async def check(*args, **kwargs):
            return await self.has_permission(*args, **kwargs) \
                   or await other.has_permission(*args, **kwargs)

        return Permission(check)


def perm(path, resolve_placeholders=None):
    path_l = path.split('.')

    async def check(permission_tree, error_reason, request, path_list=path_l):
        if resolve_placeholders:
            placeholder_vals = await resolve_placeholders(request)

        node = permission_tree

        for key in path_list[:-1]:
            if key[0] == "$":
                resolved = placeholder_vals[key[1:]]
                if isinstance(resolved, list):
                    a = True
                    for key_resolved in resolved:
                        if key_resolved not in node or not await check(node[key_resolved], error_reason, request, path_list[1:]):
                            a = False
                    return a
                else:
                    key = resolved

            if key in node:
                node = node[key]
            else:
                error_reason(path)
                return False

        return isinstance(node, list) and (path_list[-1] in node)

    return Permission(check)
