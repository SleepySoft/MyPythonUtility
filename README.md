# MyPythonUtility

I found some similar components in my different projects. I made them into utilities.

# Components

-----------------------------------------------------------------------------

## Easy Config

### File
> [easy_config.py](easy_config.py)

### Description
> A module to load and save json config. The key is in hierarchical style.
> More details can be found in the source file.

-----------------------------------------------------------------------------

## Plugin Manager

### File
> [plugin_manager.py](plugin_manager.py)

### Description
> A class used to dynamically load py files in a specified path as a plug-ins.
> It provides PluginWrapper to wrap dynamically loaded plug-ins, making calling functions in plug-ins as simple as calling ordinary class functions.
>
> More details can be found in the source file.

-----------------------------------------------------------------------------

## Hookable

### File
> [Hookable.py](Hookable.py)

### Description
> A decorator to make a function can be hooked easily. You can install a pre- / post-hook or just replace this function.
>
> More details can be found in the source file.

-----------------------------------------------------------------------------

## ObserverNotifier

### File
> [ObserverNotifier.py](ObserverNotifier.py)

### Description
> A tool that can call all observer's ```on_*``` functions through ```notify_*``` functions. 
> With this class, we don't have to write each notify function and the observer manager by manual.
>
> More details can be found in the source file.

-----------------------------------------------------------------------------

## FileBackup

### File
> [file_backup.py](file_backup.py)

### Description
> A tool for file backup. You can simply call this function to back up files in '<current_dir>/backup' 
> folder and speccify a backup limit before updating it.
> This function will automatically delete the oldest backup to fit the backup limit.
>
> More details can be found in the source file.

-----------------------------------------------------------------------------

## Arbitrary RPC

### File
> [ArbitraryRPC.py](ArbitraryRPC.py)
> [JsonSerializer.py](JsonSerializer.py)
> [JsonSerializerImpl.py](JsonSerializerImpl.py)

### Description
> A lightweight JSON-RPC framework for remote procedure calls.
> With this tool, you can call remote functions just like local ones, without defining interface declarations or writing wrappers.
> An implementation with Flask has been provided, but you can integrate with any backend.
> You can read test code as example: [arbitrary_rpc_test/test_arbitrary_rpc.py](arbitrary_rpc_test/test_arbitrary_rpc.py)
>
> More details can be found in the source file.



