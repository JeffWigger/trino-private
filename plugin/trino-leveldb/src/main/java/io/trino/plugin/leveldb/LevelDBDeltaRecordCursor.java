/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.leveldb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.DeltaRecordCursor;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalParseResult;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;

public class LevelDBDeltaRecordCursor
        implements DeltaRecordCursor
{
    private static final Splitter LINE_SPLITTER = Splitter.on("|").trimResults();

    private final List<LevelDBColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;
    private final CommunicatorFactory commFactory;
    private final Communicator comm;
    private final LevelDBTableHandle table;
    private final SimpleDateFormat formatter;
    private long totalBytes;
    private List<String> fields;

    public LevelDBDeltaRecordCursor(LevelDBTableHandle table, List<LevelDBColumnHandle> columnHandles, CommunicatorFactory commFactory)
    {
        System.out.println("LevelDBRecordCursor");
        this.columnHandles = columnHandles;
        this.table = table;
        this.formatter = new SimpleDateFormat("yyyy-MM-dd");

        fieldToColumnIndex = new int[columnHandles.size()+1];
        for (int i = 0; i < columnHandles.size(); i++) {
            LevelDBColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }
        // for the mode: Insert, Update, or Delete
        // represented by INS, UPD, DEL
        fieldToColumnIndex[fieldToColumnIndex.length-1] = fieldToColumnIndex.length-1;
        this.commFactory = commFactory;
        this.comm = commFactory.getCommunicator();

        setUpServer();
        totalBytes = 0;
    }

    private void setUpServer()
    {
        System.out.println("LevelDBRecordCursor::setUpServer");
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> map = new HashMap<String, String>();
        map.put("msg_type", "ScanTable");
        map.put("schema", this.table.getSchemaName());
        map.put("table", this.table.getTableName());
        System.out.println(mapper.valueToTree(map).asText());
        comm.write_json(mapper.valueToTree(map));
    }

    @Override
    public long getCompletedBytes()
    {
        // TODO: What is this used for
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        if (field != fieldToColumnIndex.length -1){
            checkArgument(field < columnHandles.size(), "Invalid field index");
            //System.out.println("getType:"+ columnHandles.get(field).getColumnName()+" "+columnHandles.get(field).getColumnType());
            return columnHandles.get(field).getColumnType();
        }else{
            // column that handles the mode: Insert, Update, or Delete
            return CharType.createCharType(3);
        }
    }

    @Override
    public boolean advanceNextPosition()
    {
        String line = comm.read_line();
        if (line == null) {
            // TODO: should we close comm here? Or in the close function
            System.out.println("advanceNextPosition: line was null ");
            return false;
        }
        //System.out.println("advanceNextPosition: "+ line);
        totalBytes += line.length(); // this only works for ascii encoding

        fields = LINE_SPLITTER.splitToList(line);

        return true;
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        return fields.get(columnIndex);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        //checkFieldType(field, BIGINT);
        String s = getFieldValue(field);
        //System.out.println("getLong: "+s+" was field:"+field);
        Type t = getType(field);
        if (t instanceof DecimalType) {
            DecimalType tt = (DecimalType) t;
            String prefix = "";
            String postfix = "";
            if (s.startsWith("-")) {
                prefix = "-";
                s = s.substring(1);
            }
            if (!s.contains(".")) {
                postfix = ".00";
            }
            if (s.length() > ((DecimalType) t).getPrecision()) {
                //System.out.println("MALFORMED DECIMAL");
            }
            // TODO: currently assume the decimal point is always included
            DecimalParseResult ret = Decimals.parseIncludeLeadingZerosInPrecision(prefix + "0".repeat(((DecimalType) t).getPrecision() - s.length()) + s + postfix);
            return (Long) ret.getObject();
        }
        else if (t instanceof BigintType) {
            //System.out.println("BIGINT");
            return Long.parseLong(s);
        }
        else if (t instanceof DateType) {
            Date date = null;
            try {
                date = this.formatter.parse(s);
            }
            catch (ParseException e) {
                e.printStackTrace();
                System.out.println(s +" " + Arrays.toString(fields.toArray()));
            }
            long dataLong = date.getTime();
            //System.out.println("Long_date: "+ dataLong);
            return dataLong / 86400000 + 1; // millisecond in a day //TODO why is +1 needed
        }
        return Long.parseLong(s);
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        // checkFieldType(field, createUnboundedVarcharType());
        // cannot distinguish bewteen char(25) and varchar
        Type type = getType(field);
        if (type instanceof CharType){
            CharType charType = (CharType) type;
            String small = getFieldValue(field);
            if( small.length() < charType.getLength()){
                small = small + "#".repeat(charType.getLength() - small.length());
            }
            return Slices.utf8Slice(small);
        }else if (type instanceof VarcharType){
            // Just for simpler updates
            VarcharType varcharType = (VarcharType) type;
            String small = getFieldValue(field);
            if( small.length() < varcharType.getBoundedLength()){
                small = small + "#".repeat(varcharType.getBoundedLength() - small.length());
            }
            return Slices.utf8Slice(small);
        }
        //System.out.println("LevelDBRecordCursor: got unecpected slice type");
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field)
    {
        //System.out.println("GET_OBJECT!!");
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    @Override
    public byte getByte(int field)
    {
        throw new UnsupportedOperationException();
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
        this.comm.close();
    }
}
