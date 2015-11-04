package com.AlgorithmsTests;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by Sascha Peukert on 17.10.2015.
 */
class CompareLists {

    public static boolean compareValues(Map<Integer,List<Long>> map, int sizeOfList, Long[] valuesOfList){
        for(int key :map.keySet()){
            List<Long> list = map.get(key);
            Collections.sort(list);

            if(list.size()==sizeOfList){
                int i=0; // counts the correct Entrys
                for(Long l:valuesOfList){
                    try {
                        if (list.get(i).equals(l)) {
                            i++;
                        } else {
                            break;
                        }
                    } catch (Exception e){
                        break;
                    }
                }
                if(i==sizeOfList){
                    return true;
                }
            }
        }
        return false;
    }
}
