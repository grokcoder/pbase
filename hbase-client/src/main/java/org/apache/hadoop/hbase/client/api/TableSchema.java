package org.apache.hadoop.hbase.client.api;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by wangxiaoyi on 15/6/30.
 * <p/>
 * Construct the schema of Table
 */
public class TableSchema {

    private String name;

    private static final String MESSAGE = "message";
    private static final String ROW_KEY = "required binary rowkey ;";
    private static final String TIME_STAMP = "required int64 timestamp ;";

    private static final String CF = "cf";

    private List<ColumnDescriptor> columnDescripors = null;

    public TableSchema(String name){
        this.name = name;
        this.columnDescripors = new LinkedList<>();
    }

    /**
     * add column descriptor
     * @param columnName
     * @param field_rule
     * @param field_type
     */
    public void addColumnDescriptor(String columnName, FIELD_RULE field_rule, FIELD_TYPE field_type){
        ColumnDescriptor cd = new ColumnDescriptor(columnName, field_rule, field_type);
        columnDescripors.add(cd);
    }

    /**
     * get table schema
     * @return
     */
    public String getTableSchema(){
        StringBuilder sb = new StringBuilder();
        sb.append(MESSAGE).append(" ")
                .append(name).append(" ")
                .append("{").append(" ")
                .append(ROW_KEY);

        for(ColumnDescriptor cd : columnDescripors){
            sb.append(cd.getSchemaColumnDesc());
        }
        sb.append(" ").append(TIME_STAMP).append(" }");
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public  String getCF() {
        return CF;
    }

    /**
     * descriptor for a column
     */
    class ColumnDescriptor{

        private static final String COLUMN_FAMILY = "cf:";
        private String name;
        private FIELD_TYPE field_type;
        private FIELD_RULE field_rule;


        public ColumnDescriptor(String name,  FIELD_RULE field_rule, FIELD_TYPE field_type){
            this.name = name;
            this.field_type = field_type;
            this.field_rule = field_rule;
        }

        /**
         * form schema column descriptor
         * @return
         */
        public String getSchemaColumnDesc(){
            StringBuilder sb = new StringBuilder();
            sb.append(" ").append(field_rule).append(" ")
                    .append(field_type).append(" ")
                    .append(COLUMN_FAMILY)
                    .append(name).append(" ; ");
            return sb.toString();
        }
    }


    public static void main(String []args){
        TableSchema schema = new TableSchema("people");
        schema.addColumnDescriptor("name", FIELD_RULE.repeated, FIELD_TYPE.binary);
        schema.addColumnDescriptor("age", FIELD_RULE.repeated, FIELD_TYPE.binary);
        schema.addColumnDescriptor("job", FIELD_RULE.repeated, FIELD_TYPE.binary);
        //schema.addColumnDescripor("name", FIELD_RULE.repeated, FIELD_TYPE.binary);
        System.out.print(schema.getTableSchema());
    }


}
