# This file is a part of Julia. License is MIT: http://julialang.org/license

import .Serializer: known_object_data, object_number, serialize_cycle, deserialize_cycle, writetag,
                      __deserialized_types__, serialize_typename, deserialize_typename,
                      TYPENAME_TAG, GLOBALREF_TAG, object_numbers,
                      serialize_global_from_main, deserialize_global_from_main

type ClusterSerializer{I<:IO} <: AbstractSerializer
    io::I
    counter::Int
    table::ObjectIdDict

    sent_objects::Set{UInt64} # used by serialize (track objects sent)

    ClusterSerializer(io::I) = new(io, 0, ObjectIdDict(), Set{UInt64}())
end
ClusterSerializer(io::IO) = ClusterSerializer{typeof(io)}(io)

function deserialize(s::ClusterSerializer, ::Type{TypeName})
    full_body_sent = deserialize(s)
    number = read(s.io, UInt64)
    if !full_body_sent
        tn = get(known_object_data, number, nothing)::TypeName
        if !haskey(object_numbers, tn)
            # set up reverse mapping for serialize
            object_numbers[tn] = number
        end
        deserialize_cycle(s, tn)
    else
        tn = deserialize_typename(s, number)
    end
    return tn
end

function serialize(s::ClusterSerializer, t::TypeName)
    serialize_cycle(s, t) && return
    writetag(s.io, TYPENAME_TAG)

    identifier = object_number(t)
    send_whole = !(identifier in s.sent_objects)
    serialize(s, send_whole)
    write(s.io, identifier)
    if send_whole
        serialize_typename(s, t)
        push!(s.sent_objects, identifier)
    end
#   #println(t.module, ":", t.name, ", id:", identifier, send_whole ? " sent" : " NOT sent")
    nothing
end

const FLG_SER_VAL = UInt8(1)
const FLG_SER_IDENT = UInt8(2)
const FLG_ISCONST_VAL = UInt8(4)
isflagged(v, flg) = (v & flg == flg)

function serialize_global_from_main(s::ClusterSerializer, g::GlobalRef)
    v = getfield(Main, g.name)
    # println(g)

    serialize(s, g.name)

    flags = UInt8(0)
    if isbits(v)
        identifier = 0
        flags = flags | FLG_SER_VAL
    else
        identifier = object_number(v)
        if !(identifier in s.sent_objects)
            # println("Object ", v, ", has NOT been sent previously. identifier:", identifier)
            flags = flags | FLG_SER_VAL
        end
        flags = flags | FLG_SER_IDENT
    end

    if isconst(Main, g.name)
        flags = flags | FLG_ISCONST_VAL
    end

    write(s.io, flags)

    identifier != 0 && write(s.io, identifier)
    if isflagged(flags, FLG_SER_VAL)
        serialize(s, v)
    end

    if identifier > 0
        push!(s.sent_objects, identifier)
        finalizer(v, x->release_globals_refs(s,x))
    end
end

function deserialize_global_from_main(s::ClusterSerializer)
    sym = deserialize(s)::Symbol
    flags = read(s.io, UInt8)

    identifier = 0
    if isflagged(flags, FLG_SER_IDENT)
        identifier = read(s.io, UInt64)
    end

    if isflagged(flags, FLG_SER_VAL)
        v = deserialize(s)
    else
        @assert identifier > 0
        v = get(known_object_data, identifier, nothing)
    end

    if !isbits(v) && !haskey(object_numbers, v)
        # set up reverse mapping for serialize
        object_numbers[v] = identifier
    end

    # create/update binding under Main only if the value has been sent
    if isflagged(flags, FLG_SER_VAL)
        if isflagged(flags, FLG_ISCONST_VAL)
            eval(Main, :(const $sym = $v))
        else
            eval(Main, :($sym = $v))
        end
    end

    return GlobalRef(Main, sym)
end

function release_globals_refs(s::ClusterSerializer, v)
    # TODO Run through the send objects list and delete from all nodes by setting
    # the global binding to `nothing`. Also remove from sent_objects

    # println("Released ", v)
end

