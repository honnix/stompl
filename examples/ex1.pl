:- module(ex1, []).

:- use_module(library(stompl)).

:- debug(stompl).
:- debug(ex1).

ex1 :-
    connection('192.168.99.100':32772,
               _{
                 on_connected: ex1:on_connected,
                 on_message: ex1:on_message,
                 on_disconnected: ex1:on_disconnected,
                 on_error: ex1:on_error,
                 on_heartbeat_timeout: ex1:on_heartbeat_timeout
                },
               Connection),
    setup(Connection),
    connect(Connection, '/', _{'heart-beat':'5000,5000'}),
    read(_).

on_connected(Connection, _, _) :-
    Destination = '/exchange/stompl',
    subscribe(Connection, Destination, 0, _{}).

on_message(Connection, Headers, Body) :-
    debug(ex1, 'on_message from connection ~s~n~q~n~s', [Connection, Headers, Body]),
    Destination = '/exchange/stompl_test',
    send(Connection, Destination, _{}, 'this is body'),
    send_json(Connection, Destination, _{}, _{key:value}),
    disconnect(Connection, _{}).

on_disconnected(Connection) :-
    debug(ex1, 'on_disconnected from connection ~s', [Connection]),
    teardown(Connection).

on_error(Connection, Headers, Body) :-
    debug(ex1, 'on_error from connection ~s~n~q~n~s', [Connection, Headers, Body]).

on_heartbeat_timeout(Connection) :-
    debug(ex1, 'on_heartbeat_timeout from connection ~s', [Connection]),
    teardown(Connection).