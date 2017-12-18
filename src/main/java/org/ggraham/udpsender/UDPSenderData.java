package org.ggraham.udpsender;

import org.ggraham.message.PacketDecoder;
import org.ggraham.network.UDPReceiver;
import org.ggraham.network.UDPSender;
import org.ggraham.network.UDPSender__;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.steps.userdefinedjavaclass.FieldHelper;

import java.util.Date;
import java.util.HashMap;

public class UDPSenderData extends BaseStepData implements StepDataInterface {
 
  HashMap<Integer, Integer> m_fieldMap;
  protected RowMetaInterface m_inputRowMeta;
  protected UDPSender m_sender;
  protected PacketDecoder m_decoder;
}
