import traceback


class Hookable:
    """Decorator class enabling hooks to augment or replace function behavior.

    This class allows attaching pre-hooks (executed before the wrapped function),
    post-hooks (executed after), and a replacement hook (overriding the original function).
    It supports both standalone functions and class methods via descriptor protocol.

    Attributes:
        _func (Callable): Original function being wrapped.
        _pre_hooks (List[Callable]): Hooks executed before `_func`.
        _post_hooks (List[Callable]): Hooks executed after `_func`.
        _replace_hook (Callable): Optional hook replacing `_func`.
        _hooked_instance (object): Bound instance for class methods.

    Usage:
        @Hookable
        def example(x):
            print(f"Original: {x}")

        # Add pre-hook logging
        example.add_pre_hook(lambda x: print(f"Pre-hook: {x}"))

        # Replace function behavior
        example.set_replacement_hook(lambda x: print(f"Replaced: {x * 2}"))

        example(10)
        # Output:
        #   Pre-hook: 10
        #   Replaced: 20
    """

    def __init__(self, func):
        """Initializes the Hookable decorator.

        Args:
            func (Callable): Function to be wrapped.
        """
        self._func = func
        self._pre_hooks = []
        self._post_hooks = []
        self._replace_hook = None
        self._hooked_instance = None

    def __call__(self, *args, **kwargs):
        """Executes the wrapped function with registered hooks.

        1. Runs all pre-hooks.
        2. Executes either `_replace_hook` (if set) or `_func`.
        3. Runs all post-hooks.

        Args:
            *args: Positional arguments for `_func`.
            **kwargs: Keyword arguments for `_func`.

        Returns:
            Any: Result of `_func` or `_replace_hook`.
        """
        # Pre-hooks execution
        for pre_hook in self._pre_hooks:
            pre_hook(*args, **kwargs)

        # Main function/replacement execution
        if self._hooked_instance:
            result = (
                self._func(self._hooked_instance, *args, **kwargs)
                if not self._replace_hook else
                self._replace_hook(self._hooked_instance, *args, **kwargs))
        else:
            result = (
                self._func(*args, **kwargs)
                if not self._replace_hook else
                self._replace_hook(*args, **kwargs))

        # Post-hooks execution
        for post_hook in self._post_hooks:
            post_hook(*args, **kwargs)

        return result

    def __get__(self, instance, owner):
        """Binds the method to a class instance (descriptor protocol).

        Args:
            instance (object): Instance owning the method.
            owner (type): Class of the instance.

        Returns:
            Hookable: Self with `_hooked_instance` set.
        """
        self._hooked_instance = instance
        return self

    def __repr__(self):
        """Formal string representation of the Hookable object.

        Returns:
            str: Representation including the wrapped function's name.
        """
        return f"<Hooked Function {self._func.__name__}>"

    def add_pre_hook(self, hook):
        """Registers a pre-hook function.

        Args:
            hook (Callable): Function to execute before `_func`.
        """
        self._pre_hooks.append(hook)

    def remove_pre_hook(self, hook):
        """Deregisters a pre-hook function.

        Args:
            hook (Callable): Pre-hook to remove.
        """
        self._pre_hooks.remove(hook)

    def add_post_hook(self, hook):
        """Registers a post-hook function.

        Args:
            hook (Callable): Function to execute after `_func`.
        """
        self._post_hooks.append(hook)

    def remove_post_hook(self, hook):
        """Deregisters a post-hook function.

        Args:
            hook (Callable): Post-hook to remove.
        """
        self._post_hooks.remove(hook)

    def set_replacement_hook(self, hook, force=False) -> bool:
        """Sets a hook to replace the original function.

        Args:
            hook (Callable): Function replacing `_func`.
            force (bool, optional): If `True`, overrides existing replacement.
                Defaults to `False`.

        Returns:
            bool: `True` if replacement was set, `False` if blocked by existing hook.
        """
        if self._replace_hook and not force:
            return False
        self._replace_hook = hook
        return True


# ----------------------------------------------------------------------------------------------------------------------


class Object1:
    def __init__(self):
        self.sum = 0

    @Hookable
    def obj_add(self, val):
        print(f'obj_add before: {self.sum}')
        self.sum += val
        print(f'obj_add after: {self.sum}')


g_sum = 0


@Hookable
def global_add(val):
    global g_sum
    print(f'global_add before: {g_sum}')
    g_sum += val
    print(f'global_add after: {g_sum}')


def _pre_hook(val):
    print(f'pre_hook: {val}')


def _after_hook(val):
    print(f'after_hook: {val}')


def main():
    obj = Object1()
    obj.obj_add(1)
    global_add(10)

    obj.obj_add.add_pre_hook(_pre_hook)
    obj.obj_add.add_post_hook(_after_hook)

    global_add.add_pre_hook(_pre_hook)
    global_add.add_post_hook(_after_hook)

    obj.obj_add(3)
    global_add(30)


# ----------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Error =>', e)
        print('Error =>', traceback.format_exc())
        exit()
    finally:
        pass
