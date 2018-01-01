package org.ggraham.udpsender;

import org.ggraham.ggutils.message.PacketDecoder;
import org.ggraham.ggutils.network.UDPReceiver;
import org.ggraham.ggutils.network.UDPSender;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.steps.userdefinedjavaclass.FieldHelper;

import java.util.Date;
import java.util.HashMap;

/**
 * 
 * Implements the PDI StepDataInterface
 * 
 * @author ggraham
 *
 */
public class UDPSenderData extends BaseStepData implements StepDataInterface {

	// For input row meta
	protected RowMetaInterface m_inputRowMeta;

	// UDP Sender and packet encoder 
	protected UDPSender m_sender;
	protected PacketDecoder m_decoder;
	
	// Allows for dynamic re-ordering of fields so that row order 
	// does not have to match the field order in the packet
	HashMap<Integer, Integer> m_fieldMap;
}
