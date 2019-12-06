package hello;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

import java.util.*;
import java.util.function.Function;

import javax.script.*;

public class FunctionTest {
    public static void main(String[] args) {

        List<Double> list = Arrays.asList(1.0,2.0,3.0,4.0);
        Map<String, Double> input = new HashMap<>();
        ScriptObjectMirror res;
        input.put("time", 123.123);
        Bindings bindings = new SimpleBindings();
        bindings.put("a", 1);
        String funcName = "f";
        String functor = "function f(a){\n" +
                "\tprint(a);\n" +
                "\treturn a[\"time\"];\n" +
                "}";
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
        try{
//            CompiledScript compiled = ((Compilable)engine).compile(functor);
            engine.eval(functor);
            if (engine instanceof Invocable) {
                Invocable in = (Invocable) engine;
//                res = (ScriptObjectMirror)in.invokeFunction(funcName);
                System.out.println((in.invokeFunction(funcName, input)));
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
