/*-------------------------------------------------------------------------
 *
 * vectorTupleSlot.c
 *
 * Copyright (c) 1996-2019, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "access/tuptoaster.h"
#include "executor/tuptable.h"
#include "utils/expandeddatum.h"
#include "access/vslot_tmp.h"

#include "catalog/namespace.h"
#include "executor/executor.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/hsearch.h"


static void Vslot_deform_tuple(TupleTableSlot *slot, int natts);
typedef struct VecTypeHashEntry
{
    Oid src;
    Oid dest;
}VecTypeHashEntry;
/* Map between the vectorized types and non-vectorized types */
static HTAB *hashMapN2V = NULL;
static HTAB *hashMapV2N = NULL;

const char *typenames[] = { "any", "int2", "int4", "int8", "float4", "float8",
                            "bool", "text", "date", "bpchar", "timestamp", "varchar"};
const char *vtypenames[] = { "vany", "vint2", "vint4", "vint8", "vfloat4",
                             "vfloat8", "vbool", "vtext", "vdate", "vbpchar",
                             "vtimestamp","vvarchar"};

vtype* buildvtype(Oid elemtype,int dim,bool *skip)
{
    vtype *res;
    res = palloc0(sizeof(vtype));
    res->dim = dim;
    res->elemtype = elemtype;
    res->skipref = skip;

    return res;
}

/*
 * map non-vectorized type to vectorized type.
 * To scan the PG_TYPE is inefficient, so we create a hashtable to map
 * the vectorized type and non-vectorized types.
 */
Oid GetVtype(Oid ntype)
{
    VecTypeHashEntry *entry = NULL;
    bool found = false;

    /* construct the hash table */
    if(NULL == hashMapN2V)
    {
        HASHCTL		hash_ctl;

        MemSet(&hash_ctl, 0, sizeof(hash_ctl));

        hash_ctl.keysize = sizeof(Oid);
        hash_ctl.entrysize = sizeof(VecTypeHashEntry);
        hash_ctl.hash = oid_hash;

        hashMapN2V = hash_create("vectorized_n2v",TYPE_HASH_TABLE_SIZE,
                                 &hash_ctl, HASH_ELEM | HASH_FUNCTION);
    }

    /* insert supported built-in type and vtypes */
    {
        int		i;
        Oid		vtypid;
        Oid		typid;
        for (i = 0; i < BUILTIN_TYPE_NUM; i++)
        {
            vtypid = TypenameGetTypid(vtypenames[i]);
            typid = TypenameGetTypid(typenames[i]);

            if (vtypid == InvalidOid)
                return InvalidOid;
            /* insert int4->vint4 mapping manually, may construct from catalog in future */
            entry = hash_search(hashMapN2V, &typid, HASH_ENTER, &found);
            entry->dest = vtypid;
        }
    }

    /* find the vectorized type in hash table */
    entry = hash_search(hashMapN2V, &ntype, HASH_FIND, &found);
    if(found)
        return entry->dest;

    return InvalidOid;
}

/* --------------------------------
 *		VMakeTupleTableSlot
 *
 *		Basic routine to make an empty VectorTupleTableSlot.
 * --------------------------------
 */
TupleTableSlot *
VMakeTupleTableSlot(void)
{
    TupleTableSlot		*slot;
    VectorTupleSlot		*vslot;

    slot = palloc(sizeof(VectorTupleSlot));
    NodeSetTag(slot, T_TupleTableSlot);

    slot->tts_isempty = true;
    slot->tts_shouldFree = false;
    slot->tts_shouldFreeMin = false;
    slot->tts_tuple = NULL;
    slot->tts_tupleDescriptor = NULL;
    slot->tts_mcxt = CurrentMemoryContext;
    slot->tts_buffer = InvalidBuffer;
    slot->tts_nvalid = 0;
    slot->tts_values = NULL;
    slot->tts_isnull = NULL;
    slot->tts_mintuple = NULL;

    /* vectorized fields */
    vslot = (VectorTupleSlot*)slot;
    vslot->dim = 0;
    vslot->bufnum = 0;
    memset(vslot->tts_buffers, InvalidBuffer, sizeof(vslot->tts_buffers));
    memset(vslot->tts_tuples, 0, sizeof(vslot->tts_tuples));
    /* all tuples should be skipped in initialization */
    memset(vslot->skip, true, sizeof(vslot->skip));

    return slot;
}

/* --------------------------------
 *		VExecAllocTableSlot
 *
 *		Create a vector tuple table slot within a tuple table (which is just a List).
 * --------------------------------
 */
TupleTableSlot *
VExecAllocTableSlot(List **tupleTable)
{
    TupleTableSlot *slot = VMakeTupleTableSlot();

    *tupleTable = lappend(*tupleTable, slot);

    return slot;
}


/*
 * slot_deform_tuple
 *		Given a TupleTableSlot, extract data from the slot's physical tuple
 *		into its Datum/isnull arrays.  Data is extracted up through the
 *		natts'th column (caller must ensure this is a legal column number).
 *
 *		This is essentially an incremental version of heap_deform_tuple:
 *		on each call we extract attributes up to the one needed, without
 *		re-computing information about previously extracted attributes.
 *		slot->tts_nvalid is the number of attributes already extracted.
 */
static void
Vslot_deform_tuple(TupleTableSlot *slot, int natts)
{
    VectorTupleSlot	*vslot = (VectorTupleSlot *)slot;
    TupleDesc	tupleDesc = slot->tts_tupleDescriptor;
    HeapTuple	tuple;
    HeapTupleHeader tup;
    bool		hasnulls;
    Form_pg_attribute *att = tupleDesc->attrs;
    int			attnum;
    char	   *tp;				/* ptr to tuple data */
    long		off;			/* offset in tuple data */
    bits8	   *bp;		/* ptr to null bitmap in tuple */
    bool		slow;			/* can we use/set attcacheoff? */
    int			row;
    vtype		*column;

    for (row = 0; row < vslot->dim; row++)
    {
        tuple = &vslot->tts_tuples[row];
        tup = tuple->t_data;
        bp = tup->t_bits;
        hasnulls = HeapTupleHasNulls(tuple);

        attnum = slot->tts_nvalid;
        /*
         * Check whether the first call for this tuple, and initialize or restore
         * loop state.
         */
        /* vectorize engine deform once for now */
        off = 0;
        slow = false;

        tp = (char *) tup + tup->t_hoff;

        for (; attnum < natts; attnum++)
        {
            Form_pg_attribute thisatt = att[attnum];
            column = (vtype *)slot->tts_values[attnum];

            if (hasnulls && att_isnull(attnum, bp))
            {
                column->values[row] = (Datum) 0;
                column->isnull[row] = true;
                slow = true;		/* can't use attcacheoff anymore */
                continue;
            }

            column->isnull[row] = false;

            if (!slow && thisatt->attcacheoff >= 0)
                off = thisatt->attcacheoff;
            else if (thisatt->attlen == -1)
            {
                /*
                 * We can only cache the offset for a varlena attribute if the
                 * offset is already suitably aligned, so that there would be no
                 * pad bytes in any case: then the offset will be valid for either
                 * an aligned or unaligned value.
                 */
                if (!slow &&
                    off == att_align_nominal(off, thisatt->attalign))
                    thisatt->attcacheoff = off;
                else
                {
                    off = att_align_pointer(off, thisatt->attalign, -1,
                                            tp + off);
                    slow = true;
                }
            }
            else
            {
                /* not varlena, so safe to use att_align_nominal */
                off = att_align_nominal(off, thisatt->attalign);

                if (!slow)
                    thisatt->attcacheoff = off;
            }

            column->values[row] = fetchatt(thisatt, tp + off);

            off = att_addlength_pointer(off, thisatt->attlen, tp + off);

            if (thisatt->attlen <= 0)
                slow = true;		/* can't use attcacheoff anymore */
        }
    }


    attnum = slot->tts_nvalid;
    for (; attnum < natts; attnum++)
    {
        column = (vtype *)slot->tts_values[attnum];
        column->dim = vslot->dim;
    }

    /*
     * Save state for next execution
     */
    slot->tts_nvalid = attnum;
}


/*
 * slot_getallattrs
 *		This function forces all the entries of the slot's Datum/isnull
 *		arrays to be valid.  The caller may then extract data directly
 *		from those arrays instead of using slot_getattr.
 */
void
Vslot_getallattrs(TupleTableSlot *slot)
{
    VectorTupleSlot	*vslot = (VectorTupleSlot *)slot;
    int			tdesc_natts = slot->tts_tupleDescriptor->natts;
    int			attnum;
    HeapTuple	tuple;
    int			i;

    /* Quick out if we have 'em all already */
    if (slot->tts_nvalid == tdesc_natts)
        return;

    if (vslot->dim == 0)
        return;
    /*
     * otherwise we had better have a physical tuple (tts_nvalid should equal
     * natts in all virtual-tuple cases)
     */
    for (i = 0; i < vslot->dim; i++)
    {
        tuple = &vslot->tts_tuples[i];
        if (tuple == NULL)			/* internal error */
            elog(ERROR, "cannot extract attribute from empty tuple slot");
    }
    /*
     * load up any slots available from physical tuple
     */
    attnum = HeapTupleHeaderGetNatts(vslot->tts_tuples[0].t_data);
    attnum = Min(attnum, tdesc_natts);

    Vslot_deform_tuple(slot, attnum);

    /*
     * If tuple doesn't have all the atts indicated by tupleDesc, read the
     * rest as null
     */
    for (; attnum < tdesc_natts; attnum++)
    {
        slot->tts_values[attnum] = (Datum) 0;
        slot->tts_isnull[attnum] = true;
    }
    slot->tts_nvalid = tdesc_natts;
}


/*
 * slot_getsomeattrs
 *		This function forces the entries of the slot's Datum/isnull
 *		arrays to be valid at least up through the attnum'th entry.
 */
void
Vslot_getsomeattrs(TupleTableSlot *slot, int attnum)
{
    /* Quick out if we have 'em all already */
    if (slot->tts_nvalid >= attnum)
        return;

    elog(ERROR, "slot should be deformed in scan for vectorize engine");

}


/* --------------------------------
 *		VExecClearTuple
 *
 *		This function is used to clear out a slot in the tuple table.
 *
 *		NB: only the tuple is cleared, not the tuple descriptor (if any).
 * --------------------------------
 */
TupleTableSlot *
VExecClearTuple(TupleTableSlot *slot)	/* slot in which to store tuple */
{
    int				i;
    vtype			*column;
    VectorTupleSlot *vslot;
    /*
     * sanity checks
     */
    Assert(slot != NULL);

    vslot = (VectorTupleSlot *)slot;
    /*
     * Free the old physical tuple if necessary.
     */
    if (slot->tts_shouldFree)
        heap_freetuple(slot->tts_tuple);
    if (slot->tts_shouldFreeMin)
        heap_free_minimal_tuple(slot->tts_mintuple);

    slot->tts_tuple = NULL;
    slot->tts_mintuple = NULL;
    slot->tts_shouldFree = false;
    slot->tts_shouldFreeMin = false;

    /*
     * Drop the pin on the referenced buffer, if there is one.
     */
    if (BufferIsValid(slot->tts_buffer))
        ReleaseBuffer(slot->tts_buffer);

    slot->tts_buffer = InvalidBuffer;

    /*
     * Mark it empty.
     */
    slot->tts_isempty = true;
    slot->tts_nvalid = 0;

    /* vectorize part  */
    for(i = 0; i < vslot->bufnum; i++)
    {
        if(BufferIsValid(vslot->tts_buffers[i]))
        {
            ReleaseBuffer(vslot->tts_buffers[i]);
            vslot->tts_buffers[i] = InvalidBuffer;
        }
    }
    vslot->dim = 0;
    vslot->bufnum = 0;

    for (i = 0; i < slot->tts_tupleDescriptor->natts; i++)
    {
        column = (vtype *)DatumGetPointer(slot->tts_values[i]);
        column->dim = 0;
    }

    memset(vslot->skip, true, sizeof(vslot->skip));

    return slot;
}


/* --------------------------------
 *		ExecStoreTuple
 *
 *		This function is used to store a physical tuple into a specified
 *		slot in the tuple table.
 *
 *		tuple:	tuple to store
 *		slot:	slot to store it in
 *		buffer: disk buffer if tuple is in a disk page, else InvalidBuffer
 *		shouldFree: true if ExecClearTuple should pfree() the tuple
 *					when done with it
 *
 * If 'buffer' is not InvalidBuffer, the tuple table code acquires a pin
 * on the buffer which is held until the slot is cleared, so that the tuple
 * won't go away on us.
 *
 * shouldFree is normally set 'true' for tuples constructed on-the-fly.
 * It must always be 'false' for tuples that are stored in disk pages,
 * since we don't want to try to pfree those.
 *
 * Another case where it is 'false' is when the referenced tuple is held
 * in a tuple table slot belonging to a lower-level executor Proc node.
 * In this case the lower-level slot retains ownership and responsibility
 * for eventually releasing the tuple.  When this method is used, we must
 * be certain that the upper-level Proc node will lose interest in the tuple
 * sooner than the lower-level one does!  If you're not certain, copy the
 * lower-level tuple with heap_copytuple and let the upper-level table
 * slot assume ownership of the copy!
 *
 * Return value is just the passed-in slot pointer.
 *
 * NOTE: before PostgreSQL 8.1, this function would accept a NULL tuple
 * pointer and effectively behave like ExecClearTuple (though you could
 * still specify a buffer to pin, which would be an odd combination).
 * This saved a couple lines of code in a few places, but seemed more likely
 * to mask logic errors than to be really useful, so it's now disallowed.
 * --------------------------------
 */
TupleTableSlot *
VExecStoreTuple(HeapTuple tuple,
                TupleTableSlot *slot,
                Buffer buffer,
                bool shouldFree)
{
    VectorTupleSlot *vslot;
    /*
     * sanity checks
     */
    Assert(tuple != NULL);
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);
    /* passing shouldFree=true for a tuple on a disk page is not sane */
    Assert(BufferIsValid(buffer) ? (!shouldFree) : true);

    vslot = (VectorTupleSlot *)slot;
    /*
     * Free any old physical tuple belonging to the slot.
     */
    if (slot->tts_shouldFree)
        heap_freetuple(slot->tts_tuple);
    if (slot->tts_shouldFreeMin)
        heap_free_minimal_tuple(slot->tts_mintuple);

    /*
     * Store the new tuple into the specified slot.
     */
    slot->tts_isempty = false;
    slot->tts_shouldFree = shouldFree;
    slot->tts_shouldFreeMin = false;
    memcpy(&vslot->tts_tuples[vslot->dim], tuple, sizeof(HeapTupleData));
    slot->tts_mintuple = NULL;

    /* Mark extracted state invalid */
    slot->tts_nvalid = 0;

    /*
     * If tuple is on a disk page, keep the page pinned as long as we hold a
     * pointer into it.  We assume the caller already has such a pin.
     *
     * This is coded to optimize the case where the slot previously held a
     * tuple on the same disk page: in that case releasing and re-acquiring
     * the pin is a waste of cycles.  This is a common situation during
     * seqscans, so it's worth troubling over.
     */
    if (vslot->bufnum == 0 || vslot->tts_buffers[vslot->bufnum-1] != buffer)
    {
        if (BufferIsValid(vslot->tts_buffers[vslot->bufnum]))
            ReleaseBuffer(vslot->tts_buffers[vslot->bufnum]);
        vslot->tts_buffers[vslot->bufnum] = buffer;
        vslot->bufnum++;
        if (BufferIsValid(buffer))
            IncrBufferRefCount(buffer);
    }
    vslot->dim++;

    return slot;
}

void
InitializeVectorSlotColumn(VectorTupleSlot *vslot)
{
    TupleDesc	desc;
    Oid			typid;
    vtype		*column;
    int			i;

    desc = vslot->tts.tts_tupleDescriptor;
    /* initailize column in vector slot */
    for (i = 0; i < desc->natts; i++)
    {
        typid = desc->attrs[i]->atttypid;
        column = buildvtype(typid, BATCHSIZE, vslot->skip);
        column->dim = 0;
        vslot->tts.tts_values[i]  = PointerGetDatum(column);
        /* tts_isnull not used yet */
        vslot->tts.tts_isnull[i] = false;
    }
}

TupleTableSlot *
VExecStoreInsertTuple(HeapTuple tuple,
                      TupleTableSlot *slot)
{
    VectorTupleSlot *vslot;
    /*
     * sanity checks
     */
    Assert(tuple != NULL);
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);

    vslot = (VectorTupleSlot *)slot;
    /*
     * Free any old physical tuple belonging to the slot.
     */
    if (slot->tts_shouldFree)
        heap_freetuple(slot->tts_tuple);
    if (slot->tts_shouldFreeMin)
        heap_free_minimal_tuple(slot->tts_mintuple);

    /*
     * Store the new tuple into the specified slot.
     */
    slot->tts_isempty = false;
    slot->tts_shouldFree = true;
    slot->tts_shouldFreeMin = false;
    memcpy(&vslot->tts_tuples[vslot->dim], tuple, sizeof(HeapTupleData));
    slot->tts_mintuple = NULL;

    /* Mark extracted state invalid */
    slot->tts_nvalid = 0;

    memcpy(&vslot->tts_tuples[vslot->dim], tuple, sizeof(HeapTupleData));

    /* Mark extracted state invalid */
    slot->tts_nvalid = 0;
    vslot->dim++;

    return slot;
}


VectorTuple *
Vslot_form_vtuple(VectorTupleSlot  *vslot)
{
    VectorTuple *vector_tuple;
    // calculate length
    int64_t data_length;
    int32   row;
    int32   attnum;
    long		off;			/* offset in v tuple data */
    HeapTuple	tuple;
    TupleDesc	tupleDesc;
    HeapTupleHeader tup;
    char *tp;
    Form_pg_attribute *att = tupleDesc->attrs;
    data_length = vslot->dim * 8 * vslot->tts.tts_nvalid;
    vector_tuple = palloc0(sizeof(VectorTuple) + data_length);
    vector_tuple->dim = vslot->dim;
    vector_tuple->length = data_length;
    tupleDesc = vslot->tts.tts_tupleDescriptor;

    for (attnum = 0; attnum < vslot->tts.tts_nvalid; attnum++)
    {
        Form_pg_attribute thisatt = att[attnum];
        for (row = 0; row < vslot->dim; row++) {
            tuple = &vslot->tts_tuples[row];
            tup = tuple->t_data;
            tp = (char *) tup + tup->t_hoff;
            memcpy(vector_tuple + off, (void *)fetchatt(thisatt, tp + attnum * 8), 8);
            off += 8;
        }
    }
    return vector_tuple;
}
