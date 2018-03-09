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
    path_list = path.split('.')

    async def check(permission_tree, error_reason, request):
        print(path_list, permission_tree, sep='\n')
        if resolve_placeholders:
            placeholder_vals = await resolve_placeholders(request)

        node = permission_tree

        for key in path_list[:-1]:
            if key[0] == "$":
                resolved = placeholder_vals[key[1:]]
                if isinstance(resolved, list):
                    return all(check(node[key], error_reason, request) for key in resolved)
                else:
                    key = resolved

            if key in node:
                node = node[key]
            else:
                error_reason(path)
                print('ret false')
                return False

        print('asd')
        print(isinstance(node, list), path_list[-1] in node)
        print(isinstance(node, list) and (path_list[-1] in node))
        print(isinstance(node, list) and path_list[-1] in node)

        return isinstance(node, list) and (path_list[-1] in node)

    return Permission(check)
