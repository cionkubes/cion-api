class Permission:
    """
    Represents on permission path
    """

    def __init__(self, check_fn):
        self.check_fn = check_fn

    def has_permission(self, *args, **kwargs):
        """
        Runs check function
        :return: result of check function
        """
        return self.check_fn(*args, **kwargs)

    def __and__(self, other):
        """
        Creates and returns a new permission object which runs check on self
        and given other-permissions object and returns true if self check
        **and** other check returns true.

        :param other: Permission object to compare
        :return: A new permission object with a check function that returns
            true if self or other check returns true
        """

        async def check(*args, **kwargs):
            return await self.has_permission(*args, **kwargs) \
                   and await other.has_permission(*args, **kwargs)

        return Permission(check)

    def __or__(self, other):
        """
        Creates and returns a new permission object which runs check on self
        and given other-permissions object and returns true if self check
        **or** other check returns true.

        :param other: Permission object to compare
        :return: A new permission object with a check function that returns
            true if self or other check returns true
        """

        async def check(*args, **kwargs):
            return await self.has_permission(*args, **kwargs) \
                   or await other.has_permission(*args, **kwargs)

        return Permission(check)


def perm(path, resolve_placeholders=None):
    """
    Creates and returns a permission object.

    Placeholders are values that begin with the character *$*.

    ``resolve_placeholders`` has ot be a function that takes an aiohttp request
    as a parameter and returns a dictionary containing the values for the
    placeholders defined in the path.

    So the permission-path *$env.user* would need a ``resolve_placeholders``
    function that returns a dictionary with the key *env* having a value.

    :param path: A period-separated string representing a path
    :param resolve_placeholders: A function resolve placeholders
    :return: The generated permission object
    """
    path_l = path.split('.')

    async def check(permission_tree, error_reason, request, path_list=path_l):
        if resolve_placeholders:
            placeholder_vals = await resolve_placeholders(request)

        node = permission_tree

        for i, key in enumerate(path_list[:-1]):
            if key[0] == "$":
                resolved = placeholder_vals[key[1:]]
                if isinstance(resolved, list):
                    def recur(key_resolved):
                        return check(
                            node[key_resolved],
                            error_reason,
                            request,
                            path_list=path_list[i + 1:])

                    return all(
                        [key in node and await recur(key) for key in resolved])
                else:
                    key = resolved

            if key in node:
                node = node[key]
            else:
                error_reason(path)
                return False

        return isinstance(node, list) and (path_list[-1] in node)

    return Permission(check)
