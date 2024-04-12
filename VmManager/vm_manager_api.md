## start Vm
### request
```
{
    'method': 'allocate_vm',
    'args': {
        config: {
            'name': 'process_name',
            'path': 'path_to_process',
            'command': 'command_to_run',
            'env': {
                'env1': 'value1',
                'env2': 'value2',
            }
        },
    }
    'timestamp': 'time.time()'
}
```
### response
```
{
    'request': 'entire request',
    'result': {
        'status': 'success',
        'message': 'process started successfully'
    }
    'result': {
        'status': 'failed',
        'message': 'process failed to start'
    }
}
```

## kill Vm
### request
```
{
    'method': 'remove_vm',
    'args': {
        'vm_id': vm_id
    },
    'timestamp': 'time.time()'
}
```
### response
```
{
    'request': 'entire request',
    'result': {
        'status': 'success',
        'message': 'process kill successfully'
    }
    'result': {
        'status': 'failed',
        'message': 'process failed to be killed'
    }
}
```

## reset Vm
### request
```
{
    'method': 'reset_vm',
    'args': {
        'vm_id': vm_id
    },
    'timestamp': 'time.time()'}
```
### response
```
{
    'request': 'entire request',
    'result': {
        'status': 'success',
        'message': 'process reset successfully'
    }
    'result': {
        'status': 'failed',
        'message': 'process failed to be reset'
    }
}
```

## health
### request
```
{
    'method': 'get_health',
    'timestamp': 'time.time()'
}
```
### response
```
{
    'request': 'entire request',
    'result': {
        'status': 'success',
        'health': 'good', 'bad',
    }
    'result': {
        'status': 'failed',
        'message': 'unable to get health'
    }
}
```

## Get vms
### request
```
{
    'method': 'get_vms',
    'timestamp': 'time.time()'}
```
### response
```
{
    'request': 'entire request',
    'result': {
        'status': 'success',
        'process': [],
    }
    'result': {
        'status': 'failed',
        'message': 'unable to get health'
    }
}
```