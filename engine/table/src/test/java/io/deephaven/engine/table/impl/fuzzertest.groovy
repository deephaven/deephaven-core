import io.deephaven.engine.table.impl.FailureListener
import io.deephaven.engine.table.impl.FuzzerPrintListener
import io.deephaven.engine.table.impl.TableUpdateValidator
import io.deephaven.engine.util.TableTools

tableSeed = 1583360794826387000 as long;
size = 100 as int;
scale = 1000 as int;
useRandomNullPoints = true as boolean;
tableRandom = new Random(tableSeed) as Random;

tt = TableTools.timeTable("00:00:01");
tickingValues = tt.update(
        "MyString=new String(`a`+i)",
        "MyInt=new Integer(i)",
        "MyLong=new Long(i)",
        "MyDouble=new Double(i+i/10)",
        "MyFloat=new Float(i+i/10)",
        "MyBoolean=new Boolean(i%2==0)",
        "MyChar= new Character((char) ((i%26)+97))",
        "MyShort=new Short(Integer.toString(i%32767))",
        "MyByte= new java.lang.Byte(Integer.toString(i%127))",
        "MyBigDecimal= new java.math.BigDecimal(i+i/10)",
        "MyBigInteger= new java.math.BigInteger(Integer.toString(i))"
);

nullPoints = new int[16] as int[];
if (useRandomNullPoints) {
    for (int k = 0; k < nullPoints.length; ++k) {
        nullPoints[k] = tableRandom.nextInt(60) + 4;
    }
} else {
    nullPoints = [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19] as int[];
}

randomValues1 = emptyTable(size)
        .update("Timestamp= i%nullPoints[0] == 0 ? null : new DateTime(i*1_000_000_000L)")
        .update("MyString=(i%nullPoints[1] == 0 ? null : `a`+ (tableRandom.nextInt(scale*2) - scale) )",
                "MyInt=(i%nullPoints[2] == 0 ? null : tableRandom.nextInt(scale*2) - scale )",
                "MyLong=(i%nullPoints[3] ==0 ? null : (long)(tableRandom.nextInt(scale*2) - scale))",
                "MyFloat=(float)(i%nullPoints[4] == 0 ? null : i%nullPoints[5] == 0 ? 1.0F/0.0F: i%nullPoints[6] == 0 ? -1.0F/0.0F : (tableRandom.nextFloat()-0.5)*scale)",
                "MyDouble=(double)(i%nullPoints[7] == 0 ? null : i%nullPoints[8] == 0 ? 1.0D/0.0D: i%nullPoints[9] == 0 ? -1.0D/0.0D : (tableRandom.nextDouble()-0.5)*scale)",
                "MyBoolean = (i%nullPoints[10] == 0 ? null : tableRandom.nextBoolean())",
                "MyChar = (i%nullPoints[11] == 0 ? null : new Character( (char) (tableRandom.nextInt(27)+97) ) )",
                "MyShort=(short)(i%nullPoints[12] == 0 ? null : tableRandom.nextInt(scale*2) - scale )",
                "MyByte=(Byte)(i%nullPoints[13] == 0 ? null : new Byte( Integer.toString( (int)( tableRandom.nextInt(Byte.MAX_VALUE*2)-Byte.MAX_VALUE ) ) ) )",
                "MyBigDecimal=(i%nullPoints[14] == 0 ? null : new java.math.BigDecimal( (tableRandom.nextDouble()-0.5)*scale ))",
                "MyBigInteger=(i%nullPoints[15] == 0 ? null : new java.math.BigInteger(Integer.toString(tableRandom.nextInt(scale*2) - scale) ))"
        );
randomValues = randomValues1.where("MyLong=10").view("MyLong", "Timestamp")

showWithRowSet(randomValues, 1000)


void maybeAddValidator(ArrayList<TableUpdateValidator> list, String variable) {
    def table = getBinding().getVariable(variable)
    try {
        if(table.isRefreshing()){
            validator = TableUpdateValidator.make(variable, table)
            if(null != validator) {
                list.add( validator )
                validator.getResultTable().listenForUpdates(new FailureListener())
            }
        }
    }
    catch (Exception e) {
        if(e.getClass().equals(java.lang.ArrayIndexOutOfBoundsException.class) || (e.getMessage() != null && e.getMessage().contains("nothing to validate") ) ) {
            println("skipping creating validator")
            return
        }
        log.error(e).endl();
        throw new RuntimeException(e);
    }
}

void addPrintListener(String variable, List printListeners) {
    def table = getBinding().getVariable(variable)
    pl = new FuzzerPrintListener(variable, table)
    table.listenForUpdates(pl)
    printListeners.add(pl);
}

//========================================
//Seed: 8227532519336383174
validators = []
printListeners = []

table8227532519336383174_3 = tickingValues.view("MyLong").reverse();
table8227532519336383174_7a = table8227532519336383174_3.flatten()
maybeAddValidator(validators, "table8227532519336383174_7a")
table8227532519336383174_7 = table8227532519336383174_7a.join(randomValues,"MyLong","Timestamp");
maybeAddValidator(validators, "table8227532519336383174_7")
table8227532519336383174_8a = table8227532519336383174_7.aggBy(AggLast("MyLong"))
maybeAddValidator(validators, "table8227532519336383174_8a")
addPrintListener("table8227532519336383174_8a", printListeners)