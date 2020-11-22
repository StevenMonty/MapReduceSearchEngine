import javax.swing.*;


class JobExecutor extends SwingWorker<Void, Void> {

    @Override
    protected Void doInBackground() throws Exception {
        Thread.sleep(5000);
        return null;
    }
}

//class JobExecutor extends SwingWorker<Void, Void> {
//
//    @Override
//    protected Void doInBackground() throws Exception {
//        return null;
//    }
//}
