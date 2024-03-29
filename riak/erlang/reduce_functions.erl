-module(reduce_functions).

-export([delete/2]).

% Data is a list of bucket and key pairs, intermixed with the counts of deleted
% objects. Returns a count of deleted objects.
delete(List, _None) ->
  {ok, C} = riak:local_client(),

  Delete = fun(Bucket, Key) ->
    case C:delete(Bucket, Key, 0) of
      ok -> 1;
      _ -> 0
    end
  end,

  F = fun(Elem, Acc) ->
    case Elem of
      {{Bucket, Key}, _KeyData} ->
        Acc + Delete(Bucket, Key);
      {Bucket, Key} ->
        Acc + Delete(Bucket, Key);
      [Bucket, Key] ->
        Acc + Delete(Bucket, Key);
      _ ->
        Acc + Elem
    end
  end,
  
  [lists:foldl(F, 0, List)].
