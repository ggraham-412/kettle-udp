package org.ggraham.udpreceiver;

/*
 * 
 * Apache License 2.0 
 * 
 * Copyright (c) [2017] [Gregory Graham]
 * 
 * See LICENSE.txt for details.
 * 
 */

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.ggraham.ggutils.message.PacketDecoder;
import org.ggraham.ggutils.message.FieldType;
import org.ggraham.ggutils.message.PacketFieldConfig;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * Implements StepDialogInterface
 * 
 * @author ggraham
 *
 */
public class UDPReceiverDialog extends BaseStepDialog implements StepDialogInterface {

 protected UDPReceiverMeta m_subscriberMeta;

 private CTabFolder m_wTabFolder;

 private CTabItem m_wGeneralTab;
 private TextVar m_wAddress;
 private TextVar m_wPort;
 private TextVar m_wExecuteForDuration;
 private TextVar m_wExecuteMaxPackets;

 private CTabItem m_wFieldsTab;
 private TableView m_wFieldsTable;

 private CTabItem m_wAdvancedTab;
 private TextVar m_wBufferSize;
 private TextVar m_wPoolInitSize;
 private TextVar m_wPoolMaxSize;
 private TextVar m_wThreadCount;
 private TextVar m_wPacketBufferSize;
 
 private Button m_wBigEndian;
 private Button m_wPassBinary;
 private Button m_wRepeatingGroups;
 
 public UDPReceiverDialog( Shell parent, BaseStepMeta baseStepMeta,
                              TransMeta transMeta, String stepname ) {
   super( parent, baseStepMeta, transMeta, stepname );
   m_subscriberMeta = (UDPReceiverMeta) baseStepMeta;
 }

 public UDPReceiverDialog( Shell parent, Object baseStepMeta,
                              TransMeta transMeta, String stepname ) {
   super( parent, (BaseStepMeta) baseStepMeta, transMeta, stepname );
   m_subscriberMeta = (UDPReceiverMeta) baseStepMeta;
 }

 public UDPReceiverDialog( Shell parent, int nr, BaseStepMeta in, TransMeta tr ) {
   super( parent, nr, in, tr );
   m_subscriberMeta = (UDPReceiverMeta) in;
 }

 @Override public String open() {
   Shell parent = getParent();
   Display display = parent.getDisplay();

   shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN
     | SWT.MAX );
   props.setLook( shell );
   setShellImage( shell, m_subscriberMeta );

   ModifyListener lsMod = new ModifyListener() {
     public void modifyText( ModifyEvent e ) {
       m_subscriberMeta.setChanged();
     }
   };
   changed = m_subscriberMeta.hasChanged();

   FormLayout formLayout = new FormLayout();
   formLayout.marginWidth = Const.FORM_MARGIN;
   formLayout.marginHeight = Const.FORM_MARGIN;

   shell.setLayout( formLayout );
   shell.setText( "UDP Receiver" );

   int middle = props.getMiddlePct();
   int margin = Const.MARGIN;

   // Step name
   wlStepname = new Label( shell, SWT.RIGHT );
   wlStepname.setText( BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverDialog.StepName.Label" ) );
   props.setLook( wlStepname );

   fdlStepname = new FormData();
   fdlStepname.left = new FormAttachment( 0, 0 );
   fdlStepname.right = new FormAttachment( middle, -margin );
   fdlStepname.top = new FormAttachment( 0, margin );
   wlStepname.setLayoutData( fdlStepname );
   wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
   props.setLook( wStepname );
   wStepname.addModifyListener( lsMod );
   fdStepname = new FormData();
   fdStepname.left = new FormAttachment( middle, 0 );
   fdStepname.top = new FormAttachment( 0, margin );
   fdStepname.right = new FormAttachment( 100, 0 );
   wStepname.setLayoutData( fdStepname );
   Control lastControl = wStepname;

   // ====================
   // START OF TAB FOLDER
   // ====================
   m_wTabFolder = new CTabFolder( shell, SWT.BORDER );
   props.setLook( m_wTabFolder, Props.WIDGET_STYLE_TAB );

   // ====================
   // GENERAL TAB
   // ====================

   m_wGeneralTab = new CTabItem( m_wTabFolder, SWT.NONE );
   m_wGeneralTab.setText( BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverDialog.GeneralTab.Label" ) ); 

   FormLayout mainLayout = new FormLayout();
   mainLayout.marginWidth = 3;
   mainLayout.marginHeight = 3;

   Composite wGeneralTabComp = new Composite( m_wTabFolder, SWT.NONE );
   props.setLook( wGeneralTabComp );
   wGeneralTabComp.setLayout( mainLayout );

   // IP Address
   Label wlAddress = new Label( wGeneralTabComp, SWT.RIGHT );
   wlAddress.setText( BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverDialog.Address.Label" ) );
   props.setLook( wlAddress );
   FormData fdlAddress = new FormData();
   fdlAddress.top = new FormAttachment( 0, margin * 2 );
   fdlAddress.left = new FormAttachment( 0, 0 );
   fdlAddress.right = new FormAttachment( middle, -margin );
   wlAddress.setLayoutData( fdlAddress );
   m_wAddress = new TextVar( transMeta, wGeneralTabComp, SWT.SINGLE | SWT.LEFT
     | SWT.BORDER );
   props.setLook( m_wAddress );
   m_wAddress.addModifyListener( lsMod );
   FormData fdAddress = new FormData();
   fdAddress.top = new FormAttachment( 0, margin * 2 );
   fdAddress.left = new FormAttachment( middle, 0 );
   fdAddress.right = new FormAttachment( 100, 0 );
   m_wAddress.setLayoutData( fdAddress );
   lastControl = m_wAddress;

   // Port
   Label wlPort = new Label( wGeneralTabComp, SWT.RIGHT );
   wlPort.setText( BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverDialog.Port.Label" ) );
   props.setLook( wlPort );
   FormData fdlPort = new FormData();
   fdlPort.top = new FormAttachment( lastControl, margin  );
   fdlPort.left = new FormAttachment( 0, 0 );
   fdlPort.right = new FormAttachment( middle, -margin );
   wlPort.setLayoutData( fdlPort );
   m_wPort = new TextVar( transMeta, wGeneralTabComp, SWT.SINGLE | SWT.LEFT
     | SWT.BORDER );
   props.setLook( m_wPort );
   m_wPort.addModifyListener( lsMod );
   FormData fdPort = new FormData();
   fdPort.top = new FormAttachment( lastControl, margin );
   fdPort.left = new FormAttachment( middle, 0 );
   fdPort.right = new FormAttachment( 100, 0 );
   m_wPort.setLayoutData( fdPort );
   lastControl = m_wPort;

   // Execute for duration
   Label wExecuteForLab = new Label( wGeneralTabComp, SWT.RIGHT );
   wExecuteForLab.setText( BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverDialog.MaxDuration.Label" ) );
   wExecuteForLab
     .setToolTipText( BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverDialog.MaxDuration.ToolTip" ) );
   props.setLook( wExecuteForLab );
   FormData fd = new FormData();
   fd.top = new FormAttachment( lastControl, margin );
   fd.left = new FormAttachment( 0, 0 );
   fd.right = new FormAttachment( middle, -margin );
   wExecuteForLab.setLayoutData( fd );
   m_wExecuteForDuration = new TextVar( transMeta, wGeneralTabComp, SWT.SINGLE
     | SWT.LEFT | SWT.BORDER );
   props.setLook( m_wExecuteForDuration );
   fd = new FormData();
   fd.top = new FormAttachment( lastControl, margin );
   fd.left = new FormAttachment( middle, 0 );
   fd.right = new FormAttachment( 100, 0 );
   m_wExecuteForDuration.setLayoutData( fd );
   lastControl = m_wExecuteForDuration;

   // Execute for max
   Label wExecuteMaxLab = new Label( wGeneralTabComp, SWT.RIGHT );
   wExecuteMaxLab.setText( BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverDialog.MaxPackets.Label" ) );
   wExecuteMaxLab
     .setToolTipText( BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverDialog.MaxPackets.ToolTip" ) );
   props.setLook( wExecuteMaxLab );
   fd = new FormData();
   fd.top = new FormAttachment( lastControl, margin );
   fd.left = new FormAttachment( 0, 0 );
   fd.right = new FormAttachment( middle, -margin );
   wExecuteMaxLab.setLayoutData( fd );
   m_wExecuteMaxPackets = new TextVar( transMeta, wGeneralTabComp, SWT.SINGLE
     | SWT.LEFT | SWT.BORDER );
   props.setLook( m_wExecuteMaxPackets );
   fd = new FormData();
   fd.top = new FormAttachment( lastControl, margin );
   fd.left = new FormAttachment( middle, 0 );
   fd.right = new FormAttachment( 100, 0 );
   m_wExecuteMaxPackets.setLayoutData( fd );
   lastControl = m_wExecuteMaxPackets;

   FormData fdGeneralTabComp = new FormData();
   fdGeneralTabComp.left = new FormAttachment( 0, 0 );
   fdGeneralTabComp.top = new FormAttachment( 0, 0 );
   fdGeneralTabComp.right = new FormAttachment( 100, 0 );
   fdGeneralTabComp.bottom = new FormAttachment( 100, 0 );
   wGeneralTabComp.setLayoutData( fdGeneralTabComp );

   wGeneralTabComp.layout();
   m_wGeneralTab.setControl( wGeneralTabComp );
   
   // ====================
   // Fields TAB
   // ====================

   m_wFieldsTab = new CTabItem( m_wTabFolder, SWT.NONE );
   m_wFieldsTab.setText( BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverDialog.FieldsTab.Label" ) );

   FormLayout mainLayout2 = new FormLayout();
   mainLayout2.marginWidth = 3;
   mainLayout2.marginHeight = 3;

   Composite wFieldsTabComp = new Composite( m_wTabFolder, SWT.NONE );
   props.setLook( wFieldsTabComp );
   wFieldsTabComp.setLayout( mainLayout2 );
   
   FieldType[] fTypes = FieldType.values();
   String[] fTypeNames = new String[fTypes.length];
   for ( int ii = 0; ii<fTypeNames.length; ii++) {
	   fTypeNames[ii] = fTypes[ii].toString();
   }
   
   ColumnInfo[] colinf =
		      new ColumnInfo[] {
				        new ColumnInfo( "Name", ColumnInfo.COLUMN_TYPE_TEXT ),
				        new ColumnInfo( "Type", ColumnInfo.COLUMN_TYPE_CCOMBO, fTypeNames, true ),
				        new ColumnInfo( "Field Length", ColumnInfo.COLUMN_TYPE_TEXT ),
				        new ColumnInfo( "Encoding", ColumnInfo.COLUMN_TYPE_TEXT )
		      };

		    // colinf[ 1 ].setComboValues( ValueMetaFactory.getAllValueMetaNames() );
   m_wFieldsTable =
		   new TableView( transMeta, 
				   wFieldsTabComp, 
				   SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, 
				   colinf, 5, lsMod, props );
   fd = new FormData();
   fd.left = new FormAttachment( 0, 0 );
   fd.top = new FormAttachment( lastControl, margin );
   fd.right = new FormAttachment( 100, 0 );
   fd.bottom = new FormAttachment( 100, -200 );
   m_wFieldsTable.setLayoutData( fd );

   fd = new FormData();
   fd.left = new FormAttachment( 0, 0 );
   fd.top = new FormAttachment( 0, 0 );
   fd.right = new FormAttachment( 100, 0 );
   fd.bottom = new FormAttachment( 100, 0 );
   wFieldsTabComp.setLayoutData( fd );
   wFieldsTabComp.layout();
   m_wFieldsTab.setControl( wFieldsTabComp );

   FormData fdFieldsTabComp = new FormData();
   fdFieldsTabComp.left = new FormAttachment( 0, 0 );
   fdFieldsTabComp.top = new FormAttachment( 0, 0 );
   fdFieldsTabComp.right = new FormAttachment( 100, 0 );
   fdFieldsTabComp.bottom = new FormAttachment( 100, 0 );
   wFieldsTabComp.setLayoutData( fdFieldsTabComp );

   wFieldsTabComp.layout();
   m_wFieldsTab.setControl( wFieldsTabComp );

   
   // ====================
   // Advanced TAB
   // ====================

   m_wAdvancedTab = new CTabItem( m_wTabFolder, SWT.NONE );
   m_wAdvancedTab.setText( BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverDialog.AdvancedTab.Label" ) ); 

   FormLayout mainLayout1 = new FormLayout();
   mainLayout1.marginWidth = 3;
   mainLayout1.marginHeight = 3;

   Composite wAdvancedTabComp = new Composite( m_wTabFolder, SWT.NONE );
   props.setLook( wAdvancedTabComp );
   wAdvancedTabComp.setLayout( mainLayout1 );

   // Number of Packets Buffer
   Label wlNumBuffer = new Label( wAdvancedTabComp, SWT.RIGHT );
   wlNumBuffer.setText( BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverDialog.NumBuffer.Label" ) );
   props.setLook( wlNumBuffer );
   FormData fdlNumBuffer = new FormData();
   fdlNumBuffer.top = new FormAttachment( 0, margin * 2 );
   fdlNumBuffer.left = new FormAttachment( 0, 0 );
   fdlNumBuffer.right = new FormAttachment( middle, -margin );
   wlNumBuffer.setLayoutData( fdlNumBuffer );
   m_wBufferSize = new TextVar( transMeta, wAdvancedTabComp, SWT.SINGLE | SWT.LEFT
     | SWT.BORDER );
   props.setLook( m_wBufferSize );
   m_wBufferSize.addModifyListener( lsMod );
   FormData fdNumBuffer = new FormData();
   fdNumBuffer.top = new FormAttachment( 0, margin * 2 );
   fdNumBuffer.left = new FormAttachment( middle, 0 );
   fdNumBuffer.right = new FormAttachment( 100, 0 );
   m_wBufferSize.setLayoutData( fdNumBuffer );
   lastControl = m_wBufferSize;

   // Init Pool Size
   Label wlInitPool = new Label( wAdvancedTabComp, SWT.RIGHT );
   wlInitPool.setText( BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverDialog.InitPoolSize.Label" ) );
   props.setLook( wlInitPool );
   FormData fdlInitPool = new FormData();
   fdlInitPool.top = new FormAttachment( lastControl, margin  );
   fdlInitPool.left = new FormAttachment( 0, 0 );
   fdlInitPool.right = new FormAttachment( middle, -margin );
   wlInitPool.setLayoutData( fdlInitPool );
   m_wPoolInitSize = new TextVar( transMeta, wAdvancedTabComp, SWT.SINGLE | SWT.LEFT
     | SWT.BORDER );
   props.setLook( m_wPoolInitSize );
   m_wPoolInitSize.addModifyListener( lsMod );
   FormData fdInitPool = new FormData();
   fdInitPool.top = new FormAttachment( lastControl, margin );
   fdInitPool.left = new FormAttachment( middle, 0 );
   fdInitPool.right = new FormAttachment( 100, 0 );
   m_wPoolInitSize.setLayoutData( fdInitPool );
   lastControl = m_wPoolInitSize;

   // Max Pool Size
   Label wlMaxPool = new Label( wAdvancedTabComp, SWT.RIGHT );
   wlMaxPool.setText( BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverDialog.MaxPoolSize.Label" ) );
   props.setLook( wlMaxPool );
   FormData fdlMaxPool = new FormData();
   fdlMaxPool.top = new FormAttachment( lastControl, margin  );
   fdlMaxPool.left = new FormAttachment( 0, 0 );
   fdlMaxPool.right = new FormAttachment( middle, -margin );
   wlMaxPool.setLayoutData( fdlMaxPool );
   m_wPoolMaxSize = new TextVar( transMeta, wAdvancedTabComp, SWT.SINGLE | SWT.LEFT
     | SWT.BORDER );
   props.setLook( m_wPoolMaxSize );
   m_wPoolMaxSize.addModifyListener( lsMod );
   FormData fdMaxPool = new FormData();
   fdMaxPool.top = new FormAttachment( lastControl, margin );
   fdMaxPool.left = new FormAttachment( middle, 0 );
   fdMaxPool.right = new FormAttachment( 100, 0 );
   m_wPoolMaxSize.setLayoutData( fdMaxPool );
   lastControl = m_wPoolMaxSize;

   // Thread Count
   Label wlThreadCount = new Label( wAdvancedTabComp, SWT.RIGHT );
   wlThreadCount.setText( BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverDialog.ThreadCount.Label" ) );
   props.setLook( wlThreadCount );
   FormData fdlThreadCount = new FormData();
   fdlThreadCount.top = new FormAttachment( lastControl, margin  );
   fdlThreadCount.left = new FormAttachment( 0, 0 );
   fdlThreadCount.right = new FormAttachment( middle, -margin );
   wlThreadCount.setLayoutData( fdlThreadCount );
   m_wThreadCount = new TextVar( transMeta, wAdvancedTabComp, SWT.SINGLE | SWT.LEFT
     | SWT.BORDER );
   props.setLook( m_wThreadCount );
   m_wThreadCount.addModifyListener( lsMod );
   FormData fdThreadPool = new FormData();
   fdThreadPool.top = new FormAttachment( lastControl, margin );
   fdThreadPool.left = new FormAttachment( middle, 0 );
   fdMaxPool.right = new FormAttachment( 100, 0 );
   m_wThreadCount.setLayoutData( fdThreadPool );
   lastControl = m_wThreadCount;

   // Packet Buffer
   Label wlPacketBufferSize = new Label( wAdvancedTabComp, SWT.RIGHT );
   wlPacketBufferSize.setText( BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverDialog.PacketBufferSize.Label" ) );
   props.setLook( wlPacketBufferSize );
   FormData fdlPacketBufferSize = new FormData();
   fdlPacketBufferSize.top = new FormAttachment( lastControl, margin  );
   fdlPacketBufferSize.left = new FormAttachment( 0, 0 );
   fdlPacketBufferSize.right = new FormAttachment( middle, -margin );
   wlPacketBufferSize.setLayoutData( fdlPacketBufferSize );
   m_wPacketBufferSize = new TextVar( transMeta, wAdvancedTabComp, SWT.SINGLE | SWT.LEFT
     | SWT.BORDER );
   props.setLook( m_wPacketBufferSize );
   m_wPacketBufferSize.addModifyListener( lsMod );
   FormData fdPacketBufferSize = new FormData();
   fdPacketBufferSize.top = new FormAttachment( lastControl, margin );
   fdPacketBufferSize.left = new FormAttachment( middle, 0 );
   fdPacketBufferSize.right = new FormAttachment( 100, 0 );
   m_wPacketBufferSize.setLayoutData( fdPacketBufferSize );
   lastControl = m_wPacketBufferSize;

   Label wlBigEndian = new Label( wAdvancedTabComp, SWT.RIGHT );
   wlBigEndian.setText( BaseMessages
       .getString( UDPReceiverMeta.PKG,
    		   "UDPReceiverDialog.BigEndian.Label" ) );
   props.setLook( wlBigEndian );
   fd = new FormData();
   fd.left = new FormAttachment( 0, 0 );
   fd.top = new FormAttachment( lastControl, margin * 2 );
   fd.right = new FormAttachment( middle, -margin );
   wlBigEndian.setLayoutData( fd );

   m_wBigEndian = new Button( wAdvancedTabComp, SWT.CHECK );
   props.setLook( m_wBigEndian );
   fd = new FormData();
   fd.left = new FormAttachment( middle, 0 );
   fd.top = new FormAttachment( lastControl, margin * 2 );
   fd.right = new FormAttachment( 100, 0 );
   m_wBigEndian.setLayoutData( fd );
   lastControl = m_wBigEndian;

   Label wlPassBinary = new Label( wAdvancedTabComp, SWT.RIGHT );
   wlPassBinary.setText( BaseMessages
       .getString( UDPReceiverMeta.PKG,
    		   "UDPReceiverDialog.PassBinary.Label" ) );
   props.setLook( wlPassBinary );
   fd = new FormData();
   fd.left = new FormAttachment( 0, 0 );
   fd.top = new FormAttachment( lastControl, margin * 2 );
   fd.right = new FormAttachment( middle, -margin );
   wlPassBinary.setLayoutData( fd );

   m_wPassBinary = new Button( wAdvancedTabComp, SWT.CHECK );
   props.setLook( m_wPassBinary );
   fd = new FormData();
   fd.left = new FormAttachment( middle, 0 );
   fd.top = new FormAttachment( lastControl, margin * 2 );
   fd.right = new FormAttachment( 100, 0 );
   m_wPassBinary.setLayoutData( fd );
   lastControl = m_wPassBinary;

   Label wlRepeatingGroups = new Label( wAdvancedTabComp, SWT.RIGHT );
   wlRepeatingGroups.setText( BaseMessages
       .getString( UDPReceiverMeta.PKG,
    		   "UDPReceiverDialog.RepeatingGroups.Label" ) );
   props.setLook( wlRepeatingGroups );
   fd = new FormData();
   fd.left = new FormAttachment( 0, 0 );
   fd.top = new FormAttachment( lastControl, margin * 2 );
   fd.right = new FormAttachment( middle, -margin );
   wlRepeatingGroups.setLayoutData( fd );

   m_wRepeatingGroups = new Button( wAdvancedTabComp, SWT.CHECK );
   props.setLook( m_wRepeatingGroups );
   fd = new FormData();
   fd.left = new FormAttachment( middle, 0 );
   fd.top = new FormAttachment( lastControl, margin * 2 );
   fd.right = new FormAttachment( 100, 0 );
   m_wRepeatingGroups.setLayoutData( fd );
   lastControl = m_wRepeatingGroups;

   FormData fdAdvancedTabComp = new FormData();
   fdAdvancedTabComp.left = new FormAttachment( 0, 0 );
   fdAdvancedTabComp.top = new FormAttachment( 0, 0 );
   fdAdvancedTabComp.right = new FormAttachment( 100, 0 );
   fdAdvancedTabComp.bottom = new FormAttachment( 100, 0 );
   wAdvancedTabComp.setLayoutData( fdAdvancedTabComp );

   wAdvancedTabComp.layout();
   m_wAdvancedTab.setControl( wAdvancedTabComp );

   // ====================
   // BUTTONS
   // ====================
   wOK = new Button( shell, SWT.PUSH );
   wOK.setText( BaseMessages.getString( UDPReceiverMeta.PKG, "System.Button.OK" ) ); 
   wCancel = new Button( shell, SWT.PUSH );
   wCancel.setText( BaseMessages.getString( UDPReceiverMeta.PKG, "System.Button.Cancel" ) ); 

   setButtonPositions( new Button[] { wOK, wCancel }, margin, null );

   // ====================
   // END OF TAB FOLDER
   // ====================
   FormData fdTabFolder = new FormData();
   fdTabFolder.left = new FormAttachment( 0, 0 );
   fdTabFolder.top = new FormAttachment( wStepname, margin );
   fdTabFolder.right = new FormAttachment( 100, 0 );
   fdTabFolder.bottom = new FormAttachment( wOK, -margin );
   m_wTabFolder.setLayoutData( fdTabFolder );

   // Add listeners
   lsCancel = new Listener() {
     public void handleEvent( Event e ) {
       cancel();
     }
   };
   lsOK = new Listener() {
     public void handleEvent( Event e ) {
       ok();
     }
   };
   wCancel.addListener( SWT.Selection, lsCancel );
   wOK.addListener( SWT.Selection, lsOK );

   lsDef = new SelectionAdapter() {
     public void widgetDefaultSelected( SelectionEvent e ) {
       ok();
     }
   };
   wStepname.addSelectionListener( lsDef );
   // m_wInputField.addSelectionListener( lsDef );

   m_wTabFolder.setSelection( 0 );

   // Detect X or ALT-F4 or something that kills this window...
   shell.addShellListener( new ShellAdapter() {
     public void shellClosed( ShellEvent e ) {
       cancel();
     }
   } );

   // Set the shell size, based upon previous time...
   setSize( shell, 440, 350, true );

   getData( m_subscriberMeta, true );
   m_subscriberMeta.setChanged( changed );

   shell.open();
   while ( !shell.isDisposed() ) {
     if ( !display.readAndDispatch() ) {
       display.sleep();
     }
   }
   return stepname;
 }

 private void ok() {
   if ( !Const.isEmpty( wStepname.getText() ) ) {
     setData( m_subscriberMeta );
     stepname = wStepname.getText();
     dispose();
   }
 }

 private void cancel() {
   stepname = null;
   m_subscriberMeta.setChanged( changed );
   dispose();
 }

 private void setData( UDPReceiverMeta subscriberMeta ) {
	   subscriberMeta.setAddress( m_wAddress.getText() );
	   subscriberMeta.setPort( m_wPort.getText() );
	   subscriberMeta.setMaxDuration( m_wExecuteForDuration.getText() );
	   subscriberMeta.setMaxPackets( m_wExecuteMaxPackets.getText() );
	   subscriberMeta.setBufferSize(m_wBufferSize.getText() );
	   subscriberMeta.setInitPoolSize( m_wPoolInitSize.getText() );
	   subscriberMeta.setMaxPoolSize(m_wPoolMaxSize.getText() );
	   subscriberMeta.setThreadCount(m_wThreadCount.getText());
	   subscriberMeta.setPacketBufferSize(m_wPacketBufferSize.getText());
	   subscriberMeta.setBigEndian(m_wBigEndian.getSelection());
	   subscriberMeta.setPassAsBinary(m_wPassBinary.getSelection());
	   subscriberMeta.setRepeatingGroups(m_wRepeatingGroups.getSelection());
	   
	    // fields
	    int nrNonEmptyFields = m_wFieldsTable.nrNonEmpty();
	    List<String> fieldNames = new ArrayList<String>();
	    List<PacketFieldConfig> fields = new ArrayList<PacketFieldConfig>();
	    for ( int i = 0; i < nrNonEmptyFields; i++ ) {
	      TableItem item = m_wFieldsTable.getNonEmpty( i );
	      fieldNames.add(item.getText(1).trim());
	      String fieldType = item.getText(2).trim();
	      if ( PacketFieldConfig.isSimpleType(fieldType)) {
	    	  fields.add(PacketFieldConfig.fromString(fieldType));
	    	  continue;
	      }
          StringBuilder bld = new StringBuilder(fieldType);	
          bld.append("(");          
	      String flen = item.getText(3).trim();
	      if ( flen == null || flen.isEmpty() || flen.equals("V") ) {
	    	  bld.append("V");
	      } else if ( flen.equals("R") ) {
	    	  bld.append("R");	    	  
	      } else {
	    	  int iflen = 0;
	    	  try {
	    		  iflen = Integer.parseInt(flen);	    		  
	    	  }
	    	  catch (Exception e) {
	    		  iflen = 0;
	    	  }
	    	  if ( iflen < 0 ) {
		    	  bld.append("R");	    	  	    		  
	    	  } else if ( iflen == 0 ) {
		    	  bld.append("V");	    		  
	    	  } else {
	    		  bld.append(flen);
	    	  }	    	  
	      }
	      if ( fieldType.equals("BINARY")) {
	    	  bld.append(")");
	    	  fields.add(PacketFieldConfig.fromString(bld.toString()));
	    	  continue;
	      }
	      
	      String enc = item.getText( 4 ).trim();
	      if ( enc == null || enc.isEmpty() ) {
	    	  bld.append(")");
	    	  fields.add(PacketFieldConfig.fromString(bld.toString()));
	      }
	      else {
	    	  bld.append(";").append(enc);
	    	  bld.append(")");
	    	  fields.add(PacketFieldConfig.fromString(bld.toString()));
	      }
	    }
	    subscriberMeta.setFieldNames(fieldNames.toArray(new String[] {}));
	    subscriberMeta.setFields(fields.toArray(new PacketFieldConfig[] {}));
	    
        for ( int i = 0; i < fieldNames.size(); i++ ) {
        	logBasic("Field " + fieldNames.get(i) + " set to " + fields.get(i).toString() );
        }

	   subscriberMeta.setChanged();
 }

 private void getData( UDPReceiverMeta subscriberMeta, boolean copyStepname ) {
   if ( copyStepname ) {
     wStepname.setText( stepname );
   }
   m_wAddress.setText( Const.NVL( subscriberMeta.getAddress(), "localhost" ) );
   m_wPort.setText( Const.NVL( subscriberMeta.getPort(), "0" ) );
   m_wExecuteForDuration.setText( Const.NVL( subscriberMeta.getMaxDuration(), "0" ) );
   m_wExecuteMaxPackets.setText( Const.NVL( subscriberMeta.getMaxPackets(), "0" ) );
   m_wBufferSize.setText( Const.NVL( subscriberMeta.getBufferSize(), "0" ) );
   m_wPoolInitSize.setText( Const.NVL( subscriberMeta.getInitPoolSize(), "0" ) );
   m_wPoolMaxSize.setText( Const.NVL( subscriberMeta.getMaxPoolSize(), "0" ) );
   m_wThreadCount.setText( Const.NVL( subscriberMeta.getThreadCount(), "0" ) );
   m_wPacketBufferSize.setText( Const.NVL( subscriberMeta.getPacketBufferSize(), "0" ) );
   m_wBigEndian.setSelection(subscriberMeta.getBigEndian());
   m_wRepeatingGroups.setSelection(subscriberMeta.getRepeatingGroups());
   m_wPassBinary.setSelection(subscriberMeta.getPassAsBinary());

   String[] fieldNames = subscriberMeta.getFieldNames();
   PacketFieldConfig[] fields = subscriberMeta.getFields();
   if ( fields.length > 0 ) {
     m_wFieldsTable.clearAll( false );
     Table table = m_wFieldsTable.getTable();
     for ( int i = 0; i < fields.length; i++ ) {
       TableItem item = new TableItem( table, SWT.NONE );
       item.setText( 1, fieldNames[i].trim() );
       item.setText( 2, fields[i].getFieldType().toString() );
       int flen = fields[i].getFixedLength();
       if ( flen < 0 ) {
    	   item.setText(3, "R");
       } else if (flen == 0 ) {
    	   item.setText(3, "V");
       } else {
    	   item.setText(3, (new Integer(flen)).toString()); 	   
       }
       String encoding = fields[i].getEncoding();
       if ( encoding == PacketFieldConfig.DEFAULT_ENCODING ) encoding = "";
       item.setText( 4, encoding.trim() );
     }

     m_wFieldsTable.removeEmptyRows();
     m_wFieldsTable.setRowNums();
     m_wFieldsTable.optWidth( true );
   }

 
 }
}
