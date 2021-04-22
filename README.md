# stompl

_Note: this no long works and please refer to this fork: https://github.com/JanWielemaker/stompl_

A [STOMP](http://stomp.github.io) client.

## Installation

Using SWI-Prolog 7 or later.

    ?- pack_install('https://github.com/honnix/stompl.git').

Source code available and pull requests accepted
[here](https://github.com/honnix/stompl).

@author Hongxin Liang <hxliang1982@gmail.com>

@license Apache License Version 2.0

## Supported STOMP versions

Version 1.0 and 1.1 are supported, while 1.2 is not intentionally
because I don't like '\r\n', plus it doesn't bring many new features.

## Examples

    :- module(ex, []).

    :- use_module(library(stompl)).

    ex :-
        connection('192.168.99.100':32772,
                   _{
                     on_connected: ex:on_connected,
                     on_message: ex:on_message,
                     on_disconnected: ex:on_disconnected,
                     on_error: ex:on_error,
                     on_heartbeat_timeout: ex1:on_heartbeat_timeout
                    },
                   Connection),
        setup(Connection),
        connect(Connection, '/', _{'heart-beat':'5000,5000'}),
        %% go ahead with something else
        ...

    on_connected(Connection, Headers, Body) :-
        ...

    on_message(Connection, Headers, Body) :-
        ...

    on_disconnected(Connection) :-
        ...

    on_error(Connection, Headers, Body) :-
        ...

    on_heartbeat_timeout(Connection) :-
        ...

For more examples, please check source code under `examples` directory.

## License

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
