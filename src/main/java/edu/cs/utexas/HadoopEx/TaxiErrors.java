// package edu.cs.utexas.HadoopEx;

// import org.apache.hadoop.io.FloatWritable;
// import org.apache.hadoop.io.Text;


// public class TaxiErrors implements Comparable<TaxiErrors> {

//         private final Text taxiID;
//         private final FloatArrayWritable vals;

//         public TaxiErrors(Text driverID, FloatWritable money, FloatWritable seconds) {
//             this.driverID = driverID;
            
//             FloatWritable[] store = new FloatWritable[2];
//             store[0] = money;
//             store[1] = seconds;

//             this.vals = new FloatArrayWritable(store);
//         }

//         public Text getDriverID() {
//             return driverID;
//         }

//         public float getRatio() {
//             return ((FloatWritable)vals.get()[0]).get() / ((FloatWritable)vals.get()[1]).get();
//         }

//         public FloatArrayWritable getVals() {
//             return vals;
//         }

//         public FloatWritable getMoney() {
//             return (FloatWritable) vals.get()[0];
//         }

//         public FloatWritable getSeconds() {
//             return (FloatWritable) vals.get()[1];
//         }

//         public float[] getFloatVals() {
//             float[] result = new float[2];
//             result[0] = ((FloatWritable)vals.get()[0]).get();
//             result[1] = ((FloatWritable)vals.get()[1]).get();
//             return result;
//         }

    
//     /**
//      * Compares two sort data objects by their value.
//      * @param other
//      * @return 0 if equal, negative if this < other, positive if this > other
//      */
//         @Override
//         public int compareTo(DriverEarnings other) {

//             float diff = ((FloatWritable)vals.get()[0]).get() / ((FloatWritable)vals.get()[1]).get() - ((FloatWritable)other.vals.get()[0]).get() / ((FloatWritable)other.vals.get()[1]).get();
//             if (diff > 0) {
//                 return 1;
//             } else if (diff < 0) {
//                 return -1;
//             }
//             return 0;
//         }


//         public String toString(){

//             return Float.toString(((FloatWritable)vals.get()[0]).get()  / ((FloatWritable)vals.get()[1]).get() * 60);
//         }
//     }

