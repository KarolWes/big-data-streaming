import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.getCommon().addEventType(KursAkcji.class);
        EPRuntime epRuntime = EPRuntimeProvider.getDefaultRuntime(configuration);
        EPDeployment deployment = compileAndDeploy(epRuntime, """
            @name('answer') select istream data, spolka, kursOtwarcia-min(kursOtwarcia) as roznica
            from KursAkcji(spolka='Oracle')#length(2)
            having kursOtwarcia-min(kursOtwarcia) > 0
           ;""");
        ProstyListener prostyListener = new ProstyListener();
        for (EPStatement statement : deployment.getStatements()) {
            statement.addListener(prostyListener);
        }
        CreateInputStream inputStream = new CreateInputStream();
        inputStream.generuj(epRuntime.getEventService());

    }

    public static EPDeployment compileAndDeploy(EPRuntime epRuntime, String epl) {
        EPDeploymentService deploymentService = epRuntime.getDeploymentService();
        EPDeployment deployment;

        CompilerArguments args =
                new CompilerArguments(epRuntime.getConfigurationDeepCopy());
        try {
            EPCompiled epCompiled = EPCompilerProvider.getCompiler().compile(epl, args);
            deployment = deploymentService.deploy(epCompiled);
        } catch (EPCompileException | EPDeployException e) {
            throw new RuntimeException(e);
        }
        return deployment;
    }

}
